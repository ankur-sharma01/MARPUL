// =============================================================================
// execution_engine.cpp
//
// Order execution and position-lifecycle management for the trading system's
// exec process. Subscribes to entry signals over ZeroMQ, sizes and places
// orders against the broker's REST API, tracks open positions in-process,
// and manages the exit-side order lifecycle (targets, trailing stop, and a
// resting catastrophe backstop).
//
// Signal-to-fill flow:
//   1. An entry signal arrives on the ZMQ SIGNAL.* topic.
//   2. The risk engine's kill switch is re-checked immediately before order
//      placement (it was already checked upstream, but the ZMQ hop can add
//      up to ~200ms of latency, which is long enough for conditions to
//      change).
//   3. Position size comes from the Kelly sizer.
//   4. A limit order is placed at LTP + a small buffer (wider if the book
//      looks thin), then polled for fill-or-kill within a fixed window.
//   5. On fill: the position is registered, targets are recomputed from the
//      actual fill price (not the signal's LTP), and a resting "catastrophe"
//      limit order is placed well below stop as a backstop against a total
//      loss of connectivity.
//   6. On timeout: the order is cancelled and the miss is logged upstream.
//
// Broker integration notes (Shoonya / NorenWClientAPI):
//   - All order endpoints (PlaceOrder, CancelOrder, SingleOrdHist, Logout)
//     live under https://api.shoonya.com/NorenWClientAPI.
//   - Every request body is form-encoded as `jData=<url-encoded-json>`, with
//     Content-Type: application/x-www-form-urlencoded — NOT raw JSON, even
//     though the payload itself is a JSON string. Getting the content-type
//     wrong causes the server to reject or misparse the body.
//   - Auth is a single `Authorization: Bearer <token>` header; there's no
//     legacy jKey field to also populate.
//   - Transaction side is the literal string "BUY"/"SELL" ("B"/"S" in the
//     short form used by PlaceOrder), not an integer code.
//   - Fill status comes from a separate SingleOrdHist call keyed by the
//     order id returned from PlaceOrder — there's no push notification for
//     this in the synchronous REST flow used here.
//
// Operational invariants worth keeping in mind when touching this file:
//   - Never place an entry order while open positions >= MAX_POSITIONS.
//   - The kill switch must be evaluated right before the REST call, not
//     when the signal was first observed upstream.
// =============================================================================

#include "marpul/types.hpp"
#include "marpul/constants.hpp"
#include "marpul/stock_state.hpp"
#include "marpul/stage_gates.hpp"
#include "marpul/paper_trading.hpp"

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <cmath>
#include <atomic>
#include <thread>
#include <chrono>
#include <string>

// libcurl for REST (persistent connection, keep-alive)
#include <curl/curl.h>

// hiredis for direct Redis writes (trade records)
#include <hiredis/hiredis.h>

namespace marpul {

extern SessionState g_session;

// Provided by kelly_sizer.cpp and risk_engine.cpp.
double compute_position_size_kelly(
    double capital, double entry_price, double stop_loss,
    TrackType track, bool is_duopoly, bool is_xasset,
    bool is_solo_trap, bool iep_half) noexcept;
bool risk_check_entry_allowed(uint64_t now_ns_val) noexcept;
void risk_add_pnl(double delta) noexcept;
void kelly_record_trade(float r_multiple) noexcept;

// =============================================================================
// EXECUTION CONFIG (set at startup)
// =============================================================================
struct ExecConfig {
    std::string susertoken;
    std::string uid;
    std::string actid;
    double      capital{1000000.0};  // INR; updated daily
    std::string api_base = "https://api.shoonya.com/NorenWClientAPI";
};

static ExecConfig          s_cfg;
static CURL*               s_curl    = nullptr;
static struct curl_slist*  s_headers = nullptr;

// =============================================================================
// REDIS CONNECTION (exec process — for writing completed trade records)
// This process writes trade hashes directly to Redis; the ops process
// migrates them to the durable store on a fixed schedule. Both sides agree
// on the same key scheme, so the migration path doesn't need to know
// anything special about where the record came from.
//
// Declared with external linkage (not `static`) because kelly_sizer.cpp
// shares this handle via an `extern redisContext*` declaration in another
// translation unit — a `static` variable has internal linkage, so an
// `extern` reference to it elsewhere is undefined behaviour even where a
// given toolchain happens to link it successfully today.
// =============================================================================
redisContext* s_exec_redis = nullptr;

Err exec_redis_init(const char* host, int port) noexcept {
    s_exec_redis = redisConnect(host, port);
    if (!s_exec_redis || s_exec_redis->err) {
        std::fprintf(stderr, "[Exec] Redis connect failed: %s\n",
                     s_exec_redis ? s_exec_redis->errstr : "null context");
        return Err::REDIS_ERROR;
    }
    std::fprintf(stdout, "[Exec] Redis connected %s:%d (trade recording)\n", host, port);
    return Err::OK;
}

void exec_redis_cleanup() noexcept {
    if (s_exec_redis) { redisFree(s_exec_redis); s_exec_redis = nullptr; }
}

// Write a completed trade record to Redis.
// Called from close_full() in post_entry_monitor.cpp after every full close.
// Key format: trade:<SYMBOL>:<ENTRY_NS>  (matches R_HASH_TRADE in constants.hpp)
void exec_record_completed_trade(
    const char* symbol,
    double      entry_price,
    double      exit_price,
    double      sl_original,
    double      realised_pnl,
    float       r_multiple,
    int         qty,
    int         track_type,
    int         exit_reason,
    uint64_t    entry_ns,
    uint64_t    exit_ns) noexcept
{
    if (!s_exec_redis) {
        // Should never happen in production (exec_redis_init fails fast at
        // startup), but guard defensively rather than crash on a null deref.
        std::fprintf(stderr,
            "[Exec] WARNING: s_exec_redis is null — trade %s not recorded\n",
            symbol);
        return;
    }

    char key[128];
    std::snprintf(key, sizeof(key), "%s%s:%llu",
                  constants::R_HASH_TRADE, symbol,
                  static_cast<unsigned long long>(entry_ns));

    // Synchronous redisCommand: trade records are infrequent (a few dozen a
    // day at most) and correctness matters more than latency here.
    void* reply = redisCommand(s_exec_redis,
        "HSET %s sym %s entry %.2f exit %.2f sl %.2f "
        "pnl %.2f r %.3f qty %d trk %d rsn %d "
        "ens %llu xns %llu",
        key, symbol, entry_price, exit_price, sl_original,
        realised_pnl, r_multiple, qty, track_type, exit_reason,
        static_cast<unsigned long long>(entry_ns),
        static_cast<unsigned long long>(exit_ns));

    if (reply) {
        freeReplyObject(reply);
        std::fprintf(stdout,
            "[Exec] Trade recorded to Redis: %s pnl=%.0f R=%.2f\n",
            symbol, realised_pnl, static_cast<double>(r_multiple));
    } else {
        std::fprintf(stderr,
            "[Exec] ERROR: Redis HSET failed for trade %s — reconnect needed\n",
            symbol);
        redisFree(s_exec_redis);
        s_exec_redis = nullptr;
    }
}

// =============================================================================
// ACTIVE POSITION REGISTRY (Core 2 only — single-threaded, no lock needed)
// =============================================================================
static Position s_positions[constants::MAX_POSITIONS];
static int      s_pos_count = 0;

// =============================================================================
// CURL RESPONSE BUFFER
// =============================================================================
struct CurlBuf {
    // SingleOrdHist returns the full daily order history for the account,
    // which can exceed a few KB once several orders have been placed in a
    // session — sized generously so a long history never silently truncates
    // the response (which would make a fill-status search miss a genuine
    // COMPLETE and time out a fill-or-kill order that actually filled).
    char   data[16384];
    size_t len{0};
};

static size_t curl_write_cb(char* ptr, size_t sz, size_t nmemb, void* ud) noexcept {
    CurlBuf* buf = static_cast<CurlBuf*>(ud);
    size_t total = sz * nmemb;
    if (buf->len + total < sizeof(buf->data) - 1) {
        std::memcpy(buf->data + buf->len, ptr, total);
        buf->len += total;
        buf->data[buf->len] = '\0';
    } else {
        std::fprintf(stderr,
            "[Exec] CRITICAL: CurlBuf overflow — response truncated "
            "(buf_used=%zu incoming=%zu buf_cap=%zu). Increase CurlBuf::data[] if this recurs.\n",
            buf->len, total, sizeof(buf->data));
    }
    return total;
}

// =============================================================================
// SHOONYA REST: PLACE ORDER
// Returns the broker order id on success, empty string on failure.
// =============================================================================
static std::string rest_place_order(
    const char* symbol,
    const char* exchange,    // "NSE"
    int         qty,
    double      price,
    const char* txn_type,   // "B" (buy) / "S" (sell)
    const char* order_type, // "LMT" / "MKT"
    const char* product)    // "I" (intraday MIS)
noexcept
{
    if (!s_curl) return "";

    // Fields required by the broker beyond the obvious ones:
    //   ordersource — tags the order as API-originated.
    //   dscqty      — disclosed quantity; must be present even when 0.
    //   trgprc      — trigger price; required field for every order type,
    //                 ignored by the server for plain LMT/MKT.
    //   remarks     — free-text tag, useful for reconciling this system's
    //                 orders against manual ones in the broker's order book.
    char json[640];
    std::snprintf(json, sizeof(json),
        "{\"uid\":\"%s\",\"actid\":\"%s\",\"exch\":\"%s\","
        "\"tsym\":\"%s\",\"qty\":\"%d\",\"prc\":\"%.2f\","
        "\"prd\":\"%s\",\"trantype\":\"%s\",\"prctyp\":\"%s\","
        "\"ret\":\"DAY\",\"dscqty\":\"0\",\"trgprc\":\"0.00\","
        "\"ordersource\":\"API\",\"remarks\":\"MARPUL\"}",
        s_cfg.uid.c_str(), s_cfg.actid.c_str(), exchange,
        symbol, qty, price,
        product, txn_type, order_type);

    // Body is jData=<url-encoded-json>; auth is solely the Bearer header
    // set once in exec_engine_init().
    char post[768];
    char* enc = curl_easy_escape(s_curl, json, static_cast<int>(std::strlen(json)));
    if (!enc) return "";
    std::snprintf(post, sizeof(post), "jData=%s", enc);
    curl_free(enc);

    std::string url = s_cfg.api_base + "/PlaceOrder";
    CurlBuf resp{};

    curl_easy_setopt(s_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(s_curl, CURLOPT_POSTFIELDS, post);
    curl_easy_setopt(s_curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(s_curl, CURLOPT_WRITEDATA, &resp);

    CURLcode rc = curl_easy_perform(s_curl);
    if (rc != CURLE_OK) {
        std::fprintf(stderr, "[Exec] REST placeorder failed: %s\n",
                     curl_easy_strerror(rc));
        return "";
    }

    // Success response looks like {"stat":"Ok","norenordno":"<id>"}.
    // Scanned directly rather than pulled through a JSON library — the
    // response shape is small, fixed, and this avoids a heap allocation on
    // every order placement.
    const char* field = "\"norenordno\":\"";
    const char* pos   = std::strstr(resp.data, field);
    if (!pos) {
        std::fprintf(stderr, "[Exec] placeorder no ordno in: %s\n", resp.data);
        return "";
    }
    pos += std::strlen(field);
    const char* endq = std::strchr(pos, '"');
    if (!endq) return "";
    return std::string(pos, static_cast<size_t>(endq - pos));
}

// =============================================================================
// SHOONYA REST: CANCEL ORDER
// =============================================================================
static bool rest_cancel_order(const char* norenordno) noexcept {
    if (!s_curl || !norenordno || norenordno[0] == '\0') return false;

    char json[256];
    std::snprintf(json, sizeof(json),
        "{\"uid\":\"%s\",\"norenordno\":\"%s\",\"ordersource\":\"API\"}",
        s_cfg.uid.c_str(), norenordno);

    char post[512];
    char* enc = curl_easy_escape(s_curl, json, static_cast<int>(std::strlen(json)));
    if (!enc) return false;
    std::snprintf(post, sizeof(post), "jData=%s", enc);
    curl_free(enc);

    std::string url = s_cfg.api_base + "/CancelOrder";
    CurlBuf resp{};
    curl_easy_setopt(s_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(s_curl, CURLOPT_POSTFIELDS, post);
    curl_easy_setopt(s_curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(s_curl, CURLOPT_WRITEDATA, &resp);

    CURLcode rc = curl_easy_perform(s_curl);
    // Match the exact "stat":"Ok" field rather than searching for the bare
    // substring "Ok" — the latter can appear inside an error message body
    // on a failure response and produce a false positive.
    return rc == CURLE_OK && std::strstr(resp.data, "\"stat\":\"Ok\"") != nullptr;
}

// =============================================================================
// VIRTUAL SL ORDER MANAGEMENT
//
// The stop-loss isn't resting at the broker from entry — it's tracked
// in-process and only converted into a live limit order once price moves
// into a "soft trigger" zone near the stop. This keeps the broker's order
// book quiet for the common case (price never gets near stop) while still
// giving a resting order a real chance to fill once it matters.
// =============================================================================

// Place a limit SELL order for the virtual SL's soft-trigger zone.
// Called from post_entry_monitor.cpp when ltp enters that zone.
std::string place_sl_limit_order(
    const char* symbol, int qty, double limit_price) noexcept
{
    if (is_paper_mode()) {
        static int paper_sl_seq = 0;
        char fake_id[32];
        std::snprintf(fake_id, sizeof(fake_id), "PAPER_SL_%d", ++paper_sl_seq);
        std::fprintf(stdout,
            "[Exec] PAPER virtual SL order: %s qty=%d limit=%.2f id=%s\n",
            symbol, qty, limit_price, fake_id);
        return std::string(fake_id);
    }
    char sl_tsym[32];
    std::snprintf(sl_tsym, sizeof(sl_tsym), "%s-EQ", symbol);
    return rest_place_order(sl_tsym, "NSE", qty, limit_price, "S", "LMT", "I");
}

// Cancel a virtual SL limit order (price recovered clear of the trigger
// zone, or the position closed through some other path).
//
// Returns true  = cancel confirmed by the broker.
// Returns false = REST call failed, timed out, or the broker rejected it
//                 (most likely because the order already filled).
//
// The hard-SL path in post_entry_monitor.cpp uses this return value to
// decide whether an emergency market sell is still needed:
//   cancel ok  + not filled  -> fire a market sell; the slot is genuinely open.
//   cancel ok  + filled      -> close at the limit fill price; no market sell.
//   cancel fail + filled     -> close at the limit fill price; no market sell.
//   cancel fail + not filled -> the limit order is still resting at the
//     broker. Firing a market sell here would double the exposure once both
//     orders fill; let the resting order execute naturally instead.
bool cancel_sl_order(const char* norenordno) noexcept {
    if (!norenordno || norenordno[0] == '\0') return true; // nothing to cancel
    if (is_paper_mode()) {
        std::fprintf(stdout,
            "[Exec] PAPER virtual SL cancel: %s\n", norenordno);
        return true;
    }
    bool ok = rest_cancel_order(norenordno);
    std::fprintf(stdout,
        "[Exec] Virtual SL cancel %s: %s\n",
        norenordno, ok ? "OK" : "FAILED (may have already filled)");
    return ok;
}

// Single-shot check of whether a previously placed SL limit order has
// already filled, via SingleOrdHist. Used ahead of the hard-SL path so it
// never places a second SELL against an order that's already executed.
//
// Deliberately a single HTTP round-trip with no polling loop: the hard-SL
// path needs an answer immediately, and a position with an in-flight
// question about its own protection shouldn't sit unprotected while this
// spins waiting for a fill that may never come in the current tick.
bool check_sl_order_filled(const char* norenordno) noexcept {
    if (!norenordno || norenordno[0] == '\0') return false;
    if (is_paper_mode()) return false; // paper mode's hard-SL path handles this directly
    if (!s_curl) return false;

    char json[256];
    std::snprintf(json, sizeof(json),
        "{\"uid\":\"%s\",\"norenordno\":\"%s\",\"ordersource\":\"API\"}",
        s_cfg.uid.c_str(), norenordno);
    char post[512];
    char* enc = curl_easy_escape(s_curl, json, static_cast<int>(std::strlen(json)));
    if (!enc) return false;
    std::snprintf(post, sizeof(post), "jData=%s", enc);
    curl_free(enc);

    std::string url = s_cfg.api_base + "/SingleOrdHist";
    CurlBuf resp{};
    curl_easy_setopt(s_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(s_curl, CURLOPT_POSTFIELDS, post);
    curl_easy_setopt(s_curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(s_curl, CURLOPT_WRITEDATA, &resp);

    CURLcode rc = curl_easy_perform(s_curl);
    if (rc != CURLE_OK) return false;
    return std::strstr(resp.data, "\"COMPLETE\"") != nullptr;
}

// Place the catastrophe backstop order at entry: a resting limit SELL well
// below stop (entry - CATASTROPHE_ATR_MULT x ATR). This is the last line of
// defence if the process loses connectivity entirely between the soft and
// hard stop zones — it sits at the broker independent of this process being
// alive to manage it. Cancelled in close_full() on a normal exit.
static std::string place_catastrophe_order(
    const char* symbol, int qty, double limit_price) noexcept
{
    if (is_paper_mode()) {
        static int cat_seq = 0;
        char fake_id[32];
        std::snprintf(fake_id, sizeof(fake_id), "PAPER_CAT_%d", ++cat_seq);
        std::fprintf(stdout,
            "[Exec] PAPER catastrophe order: %s qty=%d limit=%.2f id=%s\n",
            symbol, qty, limit_price, fake_id);
        return std::string(fake_id);
    }
    char json[640];
    char cat_tsym[32];
    std::snprintf(cat_tsym, sizeof(cat_tsym), "%s-EQ", symbol);
    std::snprintf(json, sizeof(json),
        "{\"uid\":\"%s\",\"actid\":\"%s\",\"exch\":\"NSE\","
        "\"tsym\":\"%s\",\"qty\":\"%d\",\"prc\":\"%.2f\","
        "\"prd\":\"I\",\"trantype\":\"S\",\"prctyp\":\"LMT\","
        "\"ret\":\"DAY\",\"dscqty\":\"0\",\"trgprc\":\"0.00\","
        "\"ordersource\":\"API\",\"remarks\":\"MARPUL_CAT\"}",
        s_cfg.uid.c_str(), s_cfg.actid.c_str(),
        cat_tsym, qty, limit_price);

    char post[768];
    char* enc = curl_easy_escape(s_curl, json, static_cast<int>(std::strlen(json)));
    if (!enc) return "";
    std::snprintf(post, sizeof(post), "jData=%s", enc);
    curl_free(enc);

    std::string url = s_cfg.api_base + "/PlaceOrder";
    CurlBuf resp{};
    curl_easy_setopt(s_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(s_curl, CURLOPT_POSTFIELDS, post);
    curl_easy_setopt(s_curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(s_curl, CURLOPT_WRITEDATA, &resp);
    CURLcode rc = curl_easy_perform(s_curl);
    if (rc != CURLE_OK) return "";

    const char* field = "\"norenordno\":\"";
    const char* pos   = std::strstr(resp.data, field);
    if (!pos) return "";
    pos += std::strlen(field);
    const char* endq = std::strchr(pos, '"');
    if (!endq) return "";
    return std::string(pos, static_cast<size_t>(endq - pos));
}

// =============================================================================
// POLL FOR FILL (fill-or-kill within a fixed window)
// Returns true if the order was fully filled.
//
// The actual fill price (Shoonya's "flprc") is captured and returned via
// an out-parameter rather than assuming the order filled exactly at the
// requested limit. A limit BUY commonly fills at or below the limit
// (price improvement), and a limit SELL at or above it; treating the limit
// price as the fill price introduces a small but systematic bias into
// every recorded R-multiple, which compounds across the Kelly sizer's
// rolling window over hundreds of trades. If flprc is absent or zero, the
// caller-supplied default (usually the limit price) is left untouched.
//
// Shoonya SingleOrdHist COMPLETE response (abridged):
//   [{"stat":"Ok","norenordno":"...","status":"COMPLETE","flprc":"524.50",
//     "fillshares":"20","qty":"20", ...}]
// =============================================================================
static bool poll_fill(const char* norenordno, double& fill_price_out) noexcept {
    if (!norenordno || norenordno[0] == '\0') return false;

    char json[256];
    std::snprintf(json, sizeof(json),
        "{\"uid\":\"%s\",\"norenordno\":\"%s\",\"ordersource\":\"API\"}",
        s_cfg.uid.c_str(), norenordno);

    char post[512];
    char* enc = curl_easy_escape(s_curl, json, static_cast<int>(std::strlen(json)));
    if (!enc) return false;
    std::snprintf(post, sizeof(post), "jData=%s", enc);
    curl_free(enc);

    std::string url = s_cfg.api_base + "/SingleOrdHist";

    auto deadline = std::chrono::steady_clock::now()
        + std::chrono::milliseconds(constants::FOK_TIMEOUT_MS);

    while (std::chrono::steady_clock::now() < deadline) {
        CurlBuf resp{};
        curl_easy_setopt(s_curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(s_curl, CURLOPT_POSTFIELDS, post);
        curl_easy_setopt(s_curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
        curl_easy_setopt(s_curl, CURLOPT_WRITEDATA, &resp);

        CURLcode rc = curl_easy_perform(s_curl);
        if (rc == CURLE_OK) {
            if (std::strstr(resp.data, "\"COMPLETE\"")) {
                const char* fp = std::strstr(resp.data, "\"flprc\":\"");
                if (fp) {
                    fp += 9; // skip past "flprc":"
                    char fval[32] = {};
                    size_t fi = 0;
                    while (fi < sizeof(fval) - 1 && fp[fi] && fp[fi] != '"') {
                        fval[fi] = fp[fi]; ++fi;
                    }
                    fval[fi] = '\0';
                    double parsed = std::atof(fval);
                    if (parsed > 0.0) fill_price_out = parsed;
                }
                return true;
            }
            if (std::strstr(resp.data, "\"REJECTED\"")) return false;
            // Bail out immediately on a definitive cancel — spinning further
            // wastes the rest of the poll window on an order that's done.
            if (std::strstr(resp.data, "\"CANCELED\"")) return false;
            if (std::strstr(resp.data, "\"CANCELLED\"")) return false;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

// =============================================================================
// HANDLE AN ENTRY SIGNAL
// =============================================================================
void handle_go_signal(
    const char* symbol,
    double      ltp,
    double      stop_loss,
    double      t1,
    double      t2,
    TrackType   track,
    bool        is_xasset,
    bool        is_duopoly,
    bool        is_solo_trap,
    bool        iep_half,
    float       alct_l1,
    float       norm_vol,
    float       xasset_z) noexcept
{
    uint64_t now = now_ns();

    // Re-validate the kill switch immediately before placing the order —
    // it was already checked upstream, but the ZMQ hop can add enough
    // latency for conditions to have changed since.
    if (!risk_check_entry_allowed(now)) {
        std::fprintf(stderr,
            "[Exec] GO signal for %s blocked by risk check\n", symbol);
        return;
    }

    // Widen the limit buffer when the book looks thin at L1, so the order
    // still has a realistic chance to fill without chasing price too far.
    double limit_buf = (alct_l1 > static_cast<float>(constants::ALCT_WIDE_THRESH))
        ? constants::LIMIT_BUFFER_WIDE
        : constants::LIMIT_BUFFER;
    double limit_price = ltp * (1.0 + limit_buf);

    double shares = compute_position_size_kelly(
        s_cfg.capital, limit_price, stop_loss,
        track, is_duopoly, is_xasset, is_solo_trap, iep_half);

    if (shares < 1.0) {
        std::fprintf(stderr, "[Exec] %s: sizing returned 0 shares\n", symbol);
        return;
    }

    int qty = static_cast<int>(shares);

    std::fprintf(stdout,
        "[Exec] Placing %s BUY %d @ %.2f SL=%.2f T1=%.2f T2=%.2f [%s]\n",
        symbol, qty, limit_price, stop_loss, t1, t2,
        track == TrackType::TRACK_1 ? "T1" :
        track == TrackType::TRACK_1_LATE ? "T1L" : "T2");

    // Paper mode simulates the fill (with a slippage model) and records to
    // the paper ledger instead of touching the broker. The position is
    // still registered in s_positions[] exactly as in live mode, so every
    // downstream exit path (SL, targets, trailing, catastrophe) exercises
    // the same code driven by real live-market ticks — the only thing
    // that's simulated is the entry/exit fill itself.
    bool filled = false;
    double fill_price = limit_price;

    if (is_paper_mode()) {
        // Shoonya's opening ticks start arriving from 09:15:00; treat the
        // first TRACK1_GATE_OFFSET_S seconds of the session as the
        // thin-liquidity opening window for the slippage model. Computed as
        // an explicit addition (session-open + offset) rather than folding
        // the offset into the seconds field of a single timestamp call, so
        // an offset that isn't in [0, 59) can't be silently misread as a
        // bounds violation and "corrected" into the wrong value.
        bool is_opening_60s = (now < (ns_at_ist_today(
            constants::SESSION_OPEN_H, constants::SESSION_OPEN_M, 0)
            + static_cast<uint64_t>(constants::TRACK1_GATE_OFFSET_S) * 1'000'000'000ULL));

        // Per-stock average volume isn't available at this layer (it lives
        // in StockState, owned by the calculation-engine process); 0.0
        // falls into the slippage model's "illiquid" tier, which is the
        // conservative choice for paper-mode estimates.
        double mu_vol = 0.0;

        PaperTrade pt = paper_enter_position(
            symbol, qty, limit_price, stop_loss, t1, t2,
            track, mu_vol, is_opening_60s);
        filled     = pt.fok_filled;
        fill_price = pt.entry_price;
    } else {
        // Shoonya requires the "-EQ" suffix on NSE equity trading symbols.
        char tsym_buf[32];
        std::snprintf(tsym_buf, sizeof(tsym_buf), "%s-EQ", symbol);

        std::string ordno = rest_place_order(
                tsym_buf, "NSE", qty, limit_price, "B", "LMT", "I");

        if (ordno.empty()) {
            std::fprintf(stderr, "[Exec] %s: placeorder failed\n", symbol);
            return;
        }

        // fill_price starts at limit_price and is only overwritten by
        // poll_fill() if the broker reports a valid non-zero flprc — see
        // poll_fill()'s header comment for why the fill price matters.
        filled = poll_fill(ordno.c_str(), fill_price);

        if (!filled) {
            // Timed out: cancel the unfilled order immediately so it can't
            // fill later at a price this process never accounted for.
            rest_cancel_order(ordno.c_str());
            std::fprintf(stderr,
                "[Exec] %s: FOK timeout — live order cancelled. Track miss logged.\n",
                symbol);
            return;
        }
    }

    // Paper mode can reject a fill too (e.g. simulated thin market at the
    // open) — mirror live mode's early return so both paths are symmetric
    // and a rejected paper fill never falls through to position
    // registration below.
    if (!filled) {
        std::fprintf(stderr,
            "[Exec] %s: paper FOK rejected (simulated thin market) — no position registered\n",
            symbol);
        return;
    }

    // ── Order filled ──────────────────────────────────────────────────────
    // Refresh `now` to the fill confirmation time rather than the original
    // signal-receipt timestamp — the REST call and fill-poll loop above can
    // together take up to a couple of seconds, and downstream consumers
    // (trade records, the minimum-hold-time exit trigger) should measure
    // from when the position actually opened.
    now = now_ns();

    {
        Position* slot = nullptr;

        // Defensive guard against a duplicate fill on a symbol that already
        // has an active position. The primary guard against this lives
        // upstream in the calculation engine, gated on its own view of
        // in-position state communicated over ZMQ; if that message is ever
        // dropped (e.g. a full socket buffer), the upstream guard can go
        // stale while this process still holds the original position open.
        // Catching it here is cheap insurance against briefly running two
        // independent exit paths — and therefore two SELL orders — against
        // the same symbol.
        for (int i = 0; i < s_pos_count; ++i) {
            if (s_positions[i].active &&
                std::strcmp(s_positions[i].symbol, symbol) == 0) {
                std::fprintf(stderr,
                    "[Exec] %s already has an active position "
                    "(slot %d, entry=%.2f qty=%d) — ignoring duplicate signal.\n",
                    symbol, i,
                    s_positions[i].entry_price,
                    s_positions[i].qty_remaining);
                return;
            }
        }

        // Prefer reusing a closed slot before growing the array.
        for (int i = 0; i < s_pos_count; ++i) {
            if (!s_positions[i].active) {
                slot = &s_positions[i];
                *slot = Position{}; // clear stale fields before reuse
                break;
            }
        }
        if (!slot && s_pos_count < static_cast<int>(constants::MAX_POSITIONS)) {
            slot = &s_positions[s_pos_count++];
        }
        if (!slot) {
            std::fprintf(stderr, "[Exec] %s: position array full — cannot register fill\n",
                         symbol);
            return;
        }

        Position& pos = *slot;
        std::strncpy(pos.symbol, symbol, 15);
        pos.active          = true;
        pos.track           = track;
        pos.entry_price     = fill_price; // actual broker fill (or paper-mode equivalent)
        pos.stop_loss       = stop_loss;
        pos.sl_original     = stop_loss;

        // Targets are recomputed from the actual fill price rather than
        // the signal's ltp, so they reflect the real entered risk.
        {
            double actual_risk = fill_price - stop_loss;
            if (actual_risk > 0.001) {
                pos.t1 = fill_price + constants::TARGET_T1_R * actual_risk;
                pos.t2 = fill_price + constants::TARGET_T2_R * actual_risk;
            } else {
                pos.t1 = t1;
                pos.t2 = t2;
            }
        }

        pos.qty             = qty;
        pos.qty_remaining   = qty;
        pos.is_xasset       = is_xasset;
        pos.entry_ns        = now;

        // ── Zone/trail initialisation ───────────────────────────────────
        pos.initial_gap     = fill_price - pos.stop_loss;
        pos.trail_sl        = pos.stop_loss;
        pos.trail_ref_price = fill_price;
        pos.trail_last_mod_ns = now;
        pos.zone            = 1;
        // Stamped here, before trail activation can overwrite initial_gap
        // with a percentage-based value — everything downstream that needs
        // "ATR at entry" reads this field rather than back-deriving it from
        // a value that may no longer represent the entry-time distance.
        pos.atr_at_entry    = (constants::SL_ATR_MULT > 0.0)
            ? static_cast<float>(pos.initial_gap / constants::SL_ATR_MULT)
            : static_cast<float>(pos.initial_gap);

        pos.normalised_vol  = norm_vol;
        pos.xasset_z_entry  = xasset_z;

        // ── Catastrophe backstop order ──────────────────────────────────
        {
            double atr_est = (constants::SL_ATR_MULT > 0.0)
                ? pos.initial_gap / constants::SL_ATR_MULT
                : pos.initial_gap;
            double cat_limit = fill_price - constants::CATASTROPHE_ATR_MULT * atr_est;
            if (cat_limit > 0.0) {
                std::string cat_id = place_catastrophe_order(symbol, qty, cat_limit);
                if (!cat_id.empty()) {
                    std::snprintf(pos.catastrophe_order_id,
                                  sizeof(pos.catastrophe_order_id),
                                  "%s", cat_id.c_str());
                    std::fprintf(stdout,
                        "[Exec] %s catastrophe order: limit=%.2f id=%s\n",
                        symbol, cat_limit, cat_id.c_str());
                } else {
                    std::fprintf(stderr,
                        "[Exec] %s WARNING: catastrophe order failed — "
                        "no backstop if connectivity lost\n", symbol);
                }
            }
        }

        g_session.open_positions.fetch_add(1, std::memory_order_relaxed);
        g_session.trades_today.fetch_add(1, std::memory_order_relaxed);

        // Mirror the session counters into Redis so external tooling
        // (dashboards, health checks) has a live view without needing to
        // talk to this process directly. 16-hour TTL clears them before
        // the next session's 09:00 run even if a shutdown is missed.
        if (s_exec_redis) {
            int open_now   = g_session.open_positions.load(std::memory_order_relaxed);
            int trades_now = g_session.trades_today.load(std::memory_order_relaxed);
            void* ro = redisCommand(s_exec_redis, "SET %s %d EX 57600",
                                    constants::R_KEY_OPEN_POSITIONS, open_now);
            if (ro) freeReplyObject(ro);
            void* rt = redisCommand(s_exec_redis, "SET %s %d EX 57600",
                                    constants::R_KEY_TRADES_TODAY, trades_now);
            if (rt) freeReplyObject(rt);
        }

        std::fprintf(stdout,
            "[Exec] %s filled: %d shares @ %.2f SL=%.2f T1=%.2f T2=%.2f "
            "gap=%.2f zone=1\n",
            symbol, qty, fill_price, stop_loss, pos.t1, pos.t2, pos.initial_gap);
    }
}

// Cancel the catastrophe backstop order on a normal exit.
void cancel_catastrophe_order(const char* norenordno) noexcept {
    if (!norenordno || norenordno[0] == '\0') return;
    if (is_paper_mode()) {
        std::fprintf(stdout, "[Exec] PAPER catastrophe cancel: %s\n", norenordno);
        return;
    }
    bool ok = rest_cancel_order(norenordno);
    std::fprintf(stdout,
        "[Exec] Catastrophe order cancel %s: %s\n",
        norenordno, ok ? "OK" : "FAILED (may have expired or filled)");
}

// =============================================================================
// EXECUTION ENGINE INIT / CLEANUP
// =============================================================================
Err exec_engine_init(
    const char* uid,
    const char* susertoken,
    const char* actid,
    double      capital) noexcept
{
    s_cfg.uid        = uid;
    s_cfg.susertoken = susertoken;
    s_cfg.actid      = actid;
    s_cfg.capital    = capital;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    s_curl = curl_easy_init();
    if (!s_curl) {
        std::fprintf(stderr, "[Exec] curl_easy_init failed\n");
        return Err::ALLOC_FAILED;
    }

    curl_easy_setopt(s_curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(s_curl, CURLOPT_TIMEOUT_MS, 2000L);
    curl_easy_setopt(s_curl, CURLOPT_CONNECTTIMEOUT_MS, 3000L);

    // Every request body here is form-encoded (jData=<url-encoded-json>),
    // so Content-Type must say so — a JSON content-type on a form-encoded
    // body causes the broker to reject or misparse every order call.
    s_headers = curl_slist_append(s_headers,
        "Content-Type: application/x-www-form-urlencoded");
    {
        std::string bearer = "Authorization: Bearer " + s_cfg.susertoken;
        s_headers = curl_slist_append(s_headers, bearer.c_str());
    }
    curl_easy_setopt(s_curl, CURLOPT_HTTPHEADER, s_headers);

    std::fprintf(stdout, "[Exec] init complete. Capital=\xE2\x82\xB9%.0f\n", capital);
    return Err::OK;
}

void exec_engine_cleanup() noexcept {
    if (s_curl) { curl_easy_cleanup(s_curl); s_curl = nullptr; }
    if (s_headers) { curl_slist_free_all(s_headers); s_headers = nullptr; }
    curl_global_cleanup();
}

// =============================================================================
// POSITION REGISTRY ACCESSORS (cross-TU access without exposing the array)
// =============================================================================
Position* get_positions() noexcept { return s_positions; }
int*      get_pos_count() noexcept { return &s_pos_count; }
int       get_pos_count_val() noexcept { return s_pos_count; }

// =============================================================================
// UPDATE POSITION LTP (called by exec/main.cpp on every tick)
// =============================================================================
void update_position_ltp(const char* symbol, double ltp) noexcept {
    for (int i = 0; i < s_pos_count; ++i) {
        if (s_positions[i].active &&
            std::strcmp(s_positions[i].symbol, symbol) == 0) {
            s_positions[i].ltp             = ltp;
            double risk = s_positions[i].entry_price - s_positions[i].sl_original;
            s_positions[i].unrealised_pnl  = (risk > 0.001)
                ? (ltp - s_positions[i].entry_price) * s_positions[i].qty_remaining
                : 0.0;
        }
    }
}

// =============================================================================
// PLACE EXIT ORDER
// Returns the fill price on success, 0.0 on failure.
//
// Exit order type depends on the reason: targets and trail-based exits use
// a limit at a principled reference price (the target itself, or the
// trail/stagnation level that was just breached) so they fill within the
// spread; everything else (EOD, rotation, the numbered triggers) goes out
// as a market order, since speed matters more than price on those paths.
// =============================================================================
double place_exit_order(
    const char* symbol, int qty, ExitReason reason) noexcept
{
    if (!symbol || qty <= 0 || !s_curl) return 0.0;

    const char* order_type = "MKT";
    if (reason == ExitReason::T1 || reason == ExitReason::T2 ||
        reason == ExitReason::TRAIL_SL ||
        reason == ExitReason::TRAIL_STAGNANT) {
        order_type = "LMT";
    }

    double exit_limit = 0.0;
    double trail_sl_price = 0.0;
    double t1_price = 0.0;
    double t2_price = 0.0;
    double position_ltp = 0.0;
    for (int i = 0; i < s_pos_count; ++i) {
        if (s_positions[i].active && std::strcmp(s_positions[i].symbol, symbol) == 0) {
            position_ltp   = s_positions[i].ltp;
            exit_limit     = position_ltp * 0.9995; // small discount = sits at the bid
            trail_sl_price = s_positions[i].trail_sl;
            t1_price       = s_positions[i].t1;
            t2_price       = s_positions[i].t2;
            break;
        }
    }

    // A trailing-stop exit means price has already traded through
    // trail_sl; using that level directly as the limit (rather than a
    // small discount off the current ltp) gives the order a real chance to
    // fill immediately on a fast-moving tape instead of chasing a price
    // that's already been passed.
    if (reason == ExitReason::TRAIL_SL && trail_sl_price > 0.0) {
        exit_limit = trail_sl_price;
    }

    // A stagnation exit means the stock has been flat, so the small bid
    // discount buys nothing and just adds float-rounding risk relative to
    // the exchange's tick size. Use the exact ltp instead.
    if (reason == ExitReason::TRAIL_STAGNANT && position_ltp > 0.0) {
        exit_limit = position_ltp;
    }

    // For T1/T2, the target price itself is a safe, reproducible limit:
    // the trigger condition for these exits is ltp having reached the
    // target, so a SELL limit placed at the target is always at or better
    // than the current price when this fires.
    if (reason == ExitReason::T1 && t1_price > 0.0) {
        exit_limit = t1_price;
    }
    if (reason == ExitReason::T2 && t2_price > 0.0) {
        exit_limit = t2_price;
    }

    std::string ordno;
    bool filled = false;

    // Paper mode: record the exit against the paper ledger and return the
    // fill price directly, no REST calls involved. Market-style exits
    // (EOD, rotation, the numbered triggers) don't have a computed
    // exit_limit above, so fall back to the position's last known ltp as
    // the fill proxy — matching what live mode effectively does for a
    // market order.
    if (is_paper_mode()) {
        double effective_exit = exit_limit;
        if (effective_exit <= 0.0) {
            for (int i = 0; i < s_pos_count; ++i) {
                if (s_positions[i].active &&
                    std::strcmp(s_positions[i].symbol, symbol) == 0) {
                    effective_exit = s_positions[i].ltp;
                    break;
                }
            }
        }
        double paper_fill = paper_exit_position(symbol, qty, reason, effective_exit);
        return (paper_fill > 0.0) ? paper_fill : effective_exit;
    }

    char exit_tsym_buf[32];
    std::snprintf(exit_tsym_buf, sizeof(exit_tsym_buf), "%s-EQ", symbol);

    // The broker rejects a market order that carries a non-zero price
    // field, so LMT and MKT exits diverge here: LMT sends exit_limit
    // as-is, MKT always sends 0.0.
    //
    // If an LMT exit somehow has no computed limit price (e.g. the
    // position lookup above found nothing — a symbol mismatch after a
    // partial close, or a restarted process losing its in-memory state),
    // downgrade to MKT rather than send a limit order the broker will
    // reject outright. An exit at an unknown price beats no exit on a
    // position that's still open.
    if (std::strcmp(order_type, "LMT") == 0 && exit_limit <= 0.0) {
        std::fprintf(stderr,
            "[Exec] %s LMT exit has zero limit price "
            "(position not found in active slots?) — downgrading to MKT. reason=%d\n",
            symbol, static_cast<int>(reason));
        order_type = "MKT";
    }
    double order_prc = (std::strcmp(order_type, "MKT") == 0) ? 0.0 : exit_limit;
    ordno = rest_place_order(
        exit_tsym_buf, "NSE", qty, order_prc, "S", order_type, "I");

    if (ordno.empty()) {
        std::fprintf(stderr,
            "[Exec] EXIT order failed: %s qty=%d reason=%d order_type=%s prc=%.2f\n",
            symbol, qty, static_cast<int>(reason), order_type, order_prc);
        return 0.0;
    }

    // Same fill-price capture rationale as the entry path: assuming the
    // exit filled exactly at the requested limit would introduce a
    // systematic bias into recorded R-multiples in whichever direction the
    // broker's price improvement happens to run for that exit type.
    double exit_actual_fill = exit_limit;
    filled = poll_fill(ordno.c_str(), exit_actual_fill);
    if (!filled) {
        rest_cancel_order(ordno.c_str());
        char mkt_tsym[32];
        std::snprintf(mkt_tsym, sizeof(mkt_tsym), "%s-EQ", symbol);
        ordno = rest_place_order(mkt_tsym, "NSE", qty, 0.0, "S", "MKT", "I");
        if (!ordno.empty()) {
            poll_fill(ordno.c_str(), exit_actual_fill);
        }
    }

    if (exit_actual_fill > 0.0) return exit_actual_fill;

    // Market-order fallback: best available proxy is the position's most
    // recent ltp. Checking `.active` here matters — a stale, reused slot
    // that still carries a matching symbol string would otherwise hand
    // back a leftover price from an earlier, unrelated trade.
    for (int i = 0; i < s_pos_count; ++i) {
        if (s_positions[i].active && std::strcmp(s_positions[i].symbol, symbol) == 0)
            return s_positions[i].ltp;
    }
    // No usable price found at all: return 0.0 so the caller's existing
    // "only close if price > 0" guard skips the close and logs a warning,
    // rather than recording a fabricated fill price that would corrupt the
    // realised P&L and the Kelly sizer's rolling statistics.
    std::fprintf(stderr,
        "[Exec] place_exit_order: symbol %s not found in s_positions — "
        "returning 0.0 (no close). This should not happen.\n", symbol);
    return 0.0;
}

} // namespace marpul
