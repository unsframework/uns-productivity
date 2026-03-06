package function

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ── Configuration ────────────────────────────────────────────────────
// Config is loaded from Valkey cache using the function name as the key.
// e.g. FUNCTION_TARGET=uns-productivity → reads fnkit:config:uns-productivity
//
// Config format (stored as JSON string in Valkey):
//
//	{
//	  "table": "uns_productivity",
//	  "topics": [
//	    "v1.0/enterprise/site1/area1/cnc-01/program",
//	    "v1.0/enterprise/site1/area1/cnc-02/program",
//	    "v1.0/enterprise/site1/area2/cnc-03/program",
//	    "v1.0/enterprise/site1/area2/cnc-04/program"
//	  ]
//	}

type productivityConfig struct {
	Table  string   `json:"table"`
	Topics []string `json:"topics"`
}

type unsFields struct {
	Enterprise string
	Site       string
	Area       string
	Machine    string
	Tag        string
}

// programPayload is the JSON structure published by uns-sim on /program topics.
type programPayload struct {
	MachineID      string  `json:"machine_id"`
	ProgramID      string  `json:"program_id"`
	ProgramName    string  `json:"program_name"`
	ProgressPct    float64 `json:"progress_pct"`
	PartsCompleted int     `json:"parts_completed"`
	PartsTarget    int     `json:"parts_target"`
	CycleTimeS     int     `json:"cycle_time_s"`
	Status         string  `json:"status"`
	Timestamp      string  `json:"timestamp"`
}

// runTracker tracks an active production run for a single machine.
type runTracker struct {
	ProgramID      string
	ProgramName    string
	PartsTarget    int
	CycleTimeS     int
	PartsCompleted int
	StartedAt      time.Time
	LastStatus     string
}

var (
	ctx       = context.Background()
	cache     *redis.Client
	db        *pgxpool.Pool
	keyPrefix string

	// Config cache
	configMu      sync.RWMutex
	cachedConfig  *productivityConfig
	configFetched time.Time
	configTTL     = 30 * time.Second

	// In-memory run trackers — keyed by topic
	runTrackers   map[string]*runTracker
	runTrackersMu sync.Mutex
)

func init() {
	// ── Cache connection ─────────────────────────────────────────────
	cacheURL := envOrDefault("CACHE_URL", "redis://fnkit-cache:6379")
	keyPrefix = envOrDefault("CACHE_KEY_PREFIX", "uns")

	opts, err := redis.ParseURL(cacheURL)
	if err != nil {
		log.Fatalf("[uns-productivity] Failed to parse CACHE_URL: %v", err)
	}
	cache = redis.NewClient(opts)

	if err := cache.Ping(ctx).Err(); err != nil {
		log.Printf("[uns-productivity] Warning: cache not reachable at %s: %v", cacheURL, err)
	} else {
		log.Printf("[uns-productivity] Connected to cache at %s", cacheURL)
	}

	// ── Postgres connection ──────────────────────────────────────────
	dbURL := envOrDefault("DATABASE_URL", "postgres://fnkit:fnkit@fnkit-postgres:5432/fnkit?sslmode=disable")
	db, err = pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("[uns-productivity] Failed to create Postgres pool: %v", err)
	}

	if err := db.Ping(ctx); err != nil {
		log.Printf("[uns-productivity] Warning: Postgres not reachable: %v", err)
	} else {
		log.Printf("[uns-productivity] Connected to Postgres")
	}

	// ── Initialize run trackers ──────────────────────────────────────
	runTrackers = make(map[string]*runTracker)

	// ── Register HTTP function ───────────────────────────────────────
	functionName := envOrDefault("FUNCTION_TARGET", "uns-productivity")
	functions.HTTP(functionName, productivityHandler)
	log.Printf("[uns-productivity] Registered HTTP function: %s", functionName)
}

// ── HTTP Handler ─────────────────────────────────────────────────────
// GET/POST /uns-productivity
//
// 1. Loads config from Valkey cache (cached 30s)
// 2. Reads all configured /program topics from Valkey cache
// 3. Tracks production runs per machine
// 4. When a program completes or changes → log the run to Postgres
// 5. Returns JSON summary

func productivityHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// 1. Load config from Valkey
	config, err := loadConfig()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to load config: %v", err),
		})
		return
	}

	if len(config.Topics) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "No topics configured",
		})
		return
	}

	// 2. Ensure table exists
	if err := ensureTable(config.Table); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to ensure table: %v", err),
		})
		return
	}

	// 3. Read all program topics from cache
	programs, err := readProgramTopics(config.Topics)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to read cache: %v", err),
		})
		return
	}

	// 4. Detect run completions/changes and log them
	now := time.Now().UTC()
	completedRuns := detectAndLogRuns(config.Table, programs, now)

	// 5. Build response
	machines := make([]map[string]interface{}, 0, len(programs))
	for topic, prog := range programs {
		uns := parseTopic(topic)
		runTrackersMu.Lock()
		rt := runTrackers[topic]
		var runDuration float64
		if rt != nil {
			runDuration = now.Sub(rt.StartedAt).Seconds()
		}
		runTrackersMu.Unlock()

		machines = append(machines, map[string]interface{}{
			"machine":         uns.Machine,
			"area":            uns.Area,
			"program_id":      prog.ProgramID,
			"program_name":    prog.ProgramName,
			"status":          prog.Status,
			"parts_completed": prog.PartsCompleted,
			"parts_target":    prog.PartsTarget,
			"progress_pct":    prog.ProgressPct,
			"run_seconds":     int(runDuration),
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"logged_runs": len(completedRuns),
		"machines":    machines,
		"completed":   completedRuns,
		"checked_at":  now.Format(time.RFC3339),
	})
}

// ── Config Loading (from Valkey) ─────────────────────────────────────

func loadConfig() (*productivityConfig, error) {
	configMu.RLock()
	if cachedConfig != nil && time.Since(configFetched) < configTTL {
		cfg := cachedConfig
		configMu.RUnlock()
		return cfg, nil
	}
	configMu.RUnlock()

	configMu.Lock()
	defer configMu.Unlock()

	if cachedConfig != nil && time.Since(configFetched) < configTTL {
		return cachedConfig, nil
	}

	configKey := "fnkit:config:" + envOrDefault("FUNCTION_TARGET", "uns-productivity")

	raw, err := cache.Get(ctx, configKey).Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("config not found at key %s — set it with: docker exec fnkit-cache valkey-cli SET %s '<json>'", configKey, configKey)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read config from cache key %s: %w", configKey, err)
	}

	var config productivityConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	if config.Table == "" {
		config.Table = "uns_productivity"
	}

	cachedConfig = &config
	configFetched = time.Now()
	log.Printf("[uns-productivity] Loaded config from %s (%d topics, table: %s)",
		configKey, len(config.Topics), config.Table)

	return &config, nil
}

// ── Cache Reading ────────────────────────────────────────────────────

func readProgramTopics(topics []string) (map[string]*programPayload, error) {
	pipe := cache.Pipeline()

	cmds := make([]*redis.StringCmd, len(topics))
	for i, topic := range topics {
		cmds[i] = pipe.Get(ctx, fmt.Sprintf("%s:data:%s", keyPrefix, topic))
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		// Pipeline may return errors for individual commands
	}
	_ = err

	programs := make(map[string]*programPayload)
	for i, topic := range topics {
		raw, err := cmds[i].Result()
		if err != nil || raw == "" {
			continue
		}

		var prog programPayload
		if err := json.Unmarshal([]byte(raw), &prog); err != nil {
			log.Printf("[uns-productivity] Failed to parse program for %s: %v", topic, err)
			continue
		}

		programs[topic] = &prog
	}

	return programs, nil
}

// ── Run Detection & Logging ──────────────────────────────────────────
// Tracks production runs per machine. A run is logged when:
//   1. Program changes (different program_id) — log the previous run
//   2. Program completes (parts_completed >= parts_target) — log the run
//   3. Program stops (status goes from RUNNING to STOPPED) — log the run

type completedRun struct {
	Machine        string  `json:"machine"`
	Area           string  `json:"area"`
	ProgramID      string  `json:"program_id"`
	ProgramName    string  `json:"program_name"`
	PartsCompleted int     `json:"parts_completed"`
	PartsTarget    int     `json:"parts_target"`
	TargetPct      float64 `json:"target_pct"`
	CycleTimePlanned int   `json:"cycle_time_planned_s"`
	RunDurationS   float64 `json:"run_duration_s"`
	PartsPerHour   float64 `json:"parts_per_hour"`
	StartedAt      string  `json:"started_at"`
	EndedAt        string  `json:"ended_at"`
	Reason         string  `json:"reason"`
}

func detectAndLogRuns(table string, programs map[string]*programPayload, now time.Time) []completedRun {
	runTrackersMu.Lock()
	defer runTrackersMu.Unlock()

	var completed []completedRun

	for topic, prog := range programs {
		rt, exists := runTrackers[topic]

		if !exists {
			// First time seeing this machine — initialise tracker
			if prog.ProgramID != "" && prog.Status == "RUNNING" {
				runTrackers[topic] = &runTracker{
					ProgramID:      prog.ProgramID,
					ProgramName:    prog.ProgramName,
					PartsTarget:    prog.PartsTarget,
					CycleTimeS:     prog.CycleTimeS,
					PartsCompleted: prog.PartsCompleted,
					StartedAt:      now,
					LastStatus:     prog.Status,
				}
				log.Printf("[uns-productivity] Initialised %s → %s (%s)", topic, prog.ProgramID, prog.ProgramName)
			}
			continue
		}

		shouldLog := false
		reason := ""

		// Check: program changed
		if prog.ProgramID != rt.ProgramID && prog.ProgramID != "" {
			shouldLog = true
			reason = "program_changed"
		}

		// Check: program completed (parts reached target)
		if prog.PartsCompleted >= prog.PartsTarget && prog.PartsTarget > 0 && rt.PartsCompleted < rt.PartsTarget {
			shouldLog = true
			reason = "completed"
		}

		// Check: program stopped (was RUNNING, now STOPPED)
		if rt.LastStatus == "RUNNING" && prog.Status == "STOPPED" && rt.PartsCompleted > 0 {
			shouldLog = true
			if reason == "" {
				reason = "stopped"
			}
		}

		if shouldLog {
			// Use the latest parts count (might be higher than tracker)
			partsCompleted := rt.PartsCompleted
			if prog.ProgramID == rt.ProgramID && prog.PartsCompleted > partsCompleted {
				partsCompleted = prog.PartsCompleted
			}

			duration := now.Sub(rt.StartedAt).Seconds()
			uns := parseTopic(topic)

			// Calculate metrics
			targetPct := 0.0
			if rt.PartsTarget > 0 {
				targetPct = math.Round(float64(partsCompleted)/float64(rt.PartsTarget)*1000) / 10
			}

			partsPerHour := 0.0
			if duration > 0 {
				partsPerHour = math.Round(float64(partsCompleted)/duration*3600*10) / 10
			}

			run := completedRun{
				Machine:          uns.Machine,
				Area:             uns.Area,
				ProgramID:        rt.ProgramID,
				ProgramName:      rt.ProgramName,
				PartsCompleted:   partsCompleted,
				PartsTarget:      rt.PartsTarget,
				TargetPct:        targetPct,
				CycleTimePlanned: rt.CycleTimeS,
				RunDurationS:     math.Round(duration*10) / 10,
				PartsPerHour:     partsPerHour,
				StartedAt:        rt.StartedAt.Format(time.RFC3339),
				EndedAt:          now.Format(time.RFC3339),
				Reason:           reason,
			}

			if err := insertRun(table, uns, run); err != nil {
				log.Printf("[uns-productivity] Failed to log run for %s: %v", topic, err)
			} else {
				completed = append(completed, run)
				log.Printf("[uns-productivity] %s: %s %s — %d/%d parts (%.1f%%) in %.0fs (%.1f parts/hr) [%s]",
					uns.Machine, rt.ProgramID, rt.ProgramName,
					partsCompleted, rt.PartsTarget, targetPct,
					duration, partsPerHour, reason)
			}
		}

		// Update tracker
		if prog.ProgramID != rt.ProgramID || shouldLog {
			// New program or run was logged — reset tracker
			if prog.ProgramID != "" && prog.Status == "RUNNING" {
				runTrackers[topic] = &runTracker{
					ProgramID:      prog.ProgramID,
					ProgramName:    prog.ProgramName,
					PartsTarget:    prog.PartsTarget,
					CycleTimeS:     prog.CycleTimeS,
					PartsCompleted: prog.PartsCompleted,
					StartedAt:      now,
					LastStatus:     prog.Status,
				}
			} else {
				delete(runTrackers, topic)
			}
		} else {
			// Same program — update parts count and status
			rt.PartsCompleted = prog.PartsCompleted
			rt.LastStatus = prog.Status
		}
	}

	return completed
}

// ── UNS Topic Parsing ───────────────────────────────────────────────

func parseTopic(topic string) unsFields {
	parts := strings.Split(topic, "/")

	fields := unsFields{
		Enterprise: "unknown",
		Site:       "unknown",
		Area:       "unknown",
		Machine:    "unknown",
		Tag:        "unknown",
	}

	if len(parts) >= 2 {
		fields.Enterprise = parts[1]
	}
	if len(parts) >= 3 {
		fields.Site = parts[2]
	}
	if len(parts) >= 4 {
		fields.Area = parts[3]
	}
	if len(parts) >= 5 {
		fields.Machine = parts[4]
	}
	if len(parts) >= 6 {
		fields.Tag = strings.Join(parts[5:], "/")
	}

	return fields
}

// ── Postgres ─────────────────────────────────────────────────────────

func ensureTable(table string) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id                   BIGSERIAL    PRIMARY KEY,
			logged_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			enterprise           TEXT         NOT NULL,
			site                 TEXT         NOT NULL,
			area                 TEXT         NOT NULL,
			machine              TEXT         NOT NULL,
			program_id           TEXT         NOT NULL,
			program_name         TEXT         NOT NULL,
			parts_completed      INT          NOT NULL,
			parts_target         INT          NOT NULL,
			target_pct           NUMERIC      NOT NULL,
			cycle_time_planned_s INT          NOT NULL,
			started_at           TIMESTAMPTZ  NOT NULL,
			ended_at             TIMESTAMPTZ  NOT NULL,
			run_duration_s       NUMERIC      NOT NULL,
			parts_per_hour       NUMERIC      NOT NULL,
			reason               TEXT         NOT NULL DEFAULT ''
		);
		CREATE INDEX IF NOT EXISTS idx_%s_time ON %s (started_at, ended_at);
		CREATE INDEX IF NOT EXISTS idx_%s_machine ON %s (enterprise, site, area, machine);
		CREATE INDEX IF NOT EXISTS idx_%s_program ON %s (program_id);
	`, table, table, table, table, table, table, table)

	_, err := db.Exec(ctx, query)
	return err
}

func insertRun(table string, uns unsFields, run completedRun) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (enterprise, site, area, machine, program_id, program_name,
			parts_completed, parts_target, target_pct, cycle_time_planned_s,
			started_at, ended_at, run_duration_s, parts_per_hour, reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
	`, table)

	_, err := db.Exec(ctx, query,
		uns.Enterprise,
		uns.Site,
		uns.Area,
		uns.Machine,
		run.ProgramID,
		run.ProgramName,
		run.PartsCompleted,
		run.PartsTarget,
		run.TargetPct,
		run.CycleTimePlanned,
		run.StartedAt,
		run.EndedAt,
		run.RunDurationS,
		run.PartsPerHour,
		run.Reason,
	)

	if err != nil {
		return fmt.Errorf("failed to insert run: %w", err)
	}

	return nil
}

// ── Helpers ──────────────────────────────────────────────────────────

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
