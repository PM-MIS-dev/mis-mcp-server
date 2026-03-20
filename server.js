import crypto from "crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express from "express";
import cors from "cors";
import { z } from "zod";

// ── Config ──
const SUPABASE_URL = "https://uhdshvkyfxzwabipkqtx.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVoZHNodmt5Znh6d2FiaXBrcXR4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzE2ODA0MCwiZXhwIjoyMDg4NzQ0MDQwfQ.FOxeK-GKmIGrAH3IxYGAx0fxHBh5obmjFc2Ytr3KA2s";
const EDGE_FUNCTION_URL = `${SUPABASE_URL}/functions/v1/ask`;
const ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVoZHNodmt5Znh6d2FiaXBrcXR4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMxNjgwNDAsImV4cCI6MjA4ODc0NDA0MH0.tKwF_H1n5OqDEKLyFsLjF77B40nvwibOTBmThmaeZng";

// ── Overload alert config ──
const ALERT_EMAIL = process.env.ALERT_EMAIL || "phfmphfm@gmail.com";
const ALERT_WEBHOOK = process.env.ALERT_WEBHOOK || ""; // Optional webhook URL
const OVERLOAD_THRESHOLDS = {
  maxConcurrentSessions: 20,     // Alert when this many sessions are active
  maxRequestsPerMinute: 60,      // Alert when RPM exceeds this
  maxErrorRatePercent: 25,       // Alert when error rate exceeds this %
  maxAvgResponseTimeMs: 15000,   // Alert when avg response time exceeds 15s
};
const ALERT_COOLDOWN_MS = 30 * 60 * 1000; // Don't re-alert for 30 minutes
let lastAlertTime = 0;

// ── Session & metrics tracking ──
const sessionMeta = {};  // sessionId -> { connectedAt, lastActive, requestCount, lastTool, userAgent, ip }
const serverMetrics = {
  startedAt: new Date().toISOString(),
  totalRequests: 0,
  totalErrors: 0,
  toolCalls: {},         // toolName -> count
  requestsLastMinute: [], // timestamps of recent requests
  responseTimes: [],      // last 100 response times in ms
};

function trackRequest(sessionId, toolName, durationMs, isError = false) {
  const now = Date.now();
  serverMetrics.totalRequests++;
  if (isError) serverMetrics.totalErrors++;

  // Track tool usage
  if (toolName) {
    serverMetrics.toolCalls[toolName] = (serverMetrics.toolCalls[toolName] || 0) + 1;
  }

  // Track RPM (sliding window)
  serverMetrics.requestsLastMinute.push(now);
  serverMetrics.requestsLastMinute = serverMetrics.requestsLastMinute.filter(t => now - t < 60000);

  // Track response times (keep last 100)
  if (durationMs !== undefined) {
    serverMetrics.responseTimes.push(durationMs);
    if (serverMetrics.responseTimes.length > 100) serverMetrics.responseTimes.shift();
  }

  // Update session meta
  if (sessionId && sessionMeta[sessionId]) {
    sessionMeta[sessionId].lastActive = new Date().toISOString();
    sessionMeta[sessionId].requestCount++;
    if (toolName) sessionMeta[sessionId].lastTool = toolName;
  }

  // Check overload conditions
  checkOverload();
}

function checkOverload() {
  const now = Date.now();
  if (now - lastAlertTime < ALERT_COOLDOWN_MS) return; // Respect cooldown

  const activeSessions = Object.keys(transports).length;
  const rpm = serverMetrics.requestsLastMinute.length;
  const errorRate = serverMetrics.totalRequests > 0
    ? (serverMetrics.totalErrors / serverMetrics.totalRequests) * 100
    : 0;
  const avgResponseTime = serverMetrics.responseTimes.length > 0
    ? serverMetrics.responseTimes.reduce((a, b) => a + b, 0) / serverMetrics.responseTimes.length
    : 0;

  const alerts = [];
  if (activeSessions >= OVERLOAD_THRESHOLDS.maxConcurrentSessions) {
    alerts.push(`Sessions: ${activeSessions} (threshold: ${OVERLOAD_THRESHOLDS.maxConcurrentSessions})`);
  }
  if (rpm >= OVERLOAD_THRESHOLDS.maxRequestsPerMinute) {
    alerts.push(`RPM: ${rpm} (threshold: ${OVERLOAD_THRESHOLDS.maxRequestsPerMinute})`);
  }
  if (errorRate >= OVERLOAD_THRESHOLDS.maxErrorRatePercent && serverMetrics.totalRequests > 10) {
    alerts.push(`Error rate: ${errorRate.toFixed(1)}% (threshold: ${OVERLOAD_THRESHOLDS.maxErrorRatePercent}%)`);
  }
  if (avgResponseTime >= OVERLOAD_THRESHOLDS.maxAvgResponseTimeMs && serverMetrics.responseTimes.length > 5) {
    alerts.push(`Avg response time: ${(avgResponseTime / 1000).toFixed(1)}s (threshold: ${(OVERLOAD_THRESHOLDS.maxAvgResponseTimeMs / 1000).toFixed(0)}s)`);
  }

  if (alerts.length > 0) {
    lastAlertTime = now;
    const message = `MIS MCP Server Overload Alert\n\n${alerts.join("\n")}\n\nActive sessions: ${activeSessions}\nRPM: ${rpm}\nUptime: ${getUptime()}\nTop tools: ${getTopTools(3)}`;
    console.warn("[OVERLOAD ALERT]", message);
    sendAlertEmail(message);
  }
}

async function sendAlertEmail(message) {
  // Log to Supabase alert_log table for persistence
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/mcp_alerts`, {
      method: "POST",
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=minimal",
      },
      body: JSON.stringify({
        alert_type: "overload",
        message,
        active_sessions: Object.keys(transports).length,
        rpm: serverMetrics.requestsLastMinute.length,
        created_at: new Date().toISOString(),
      }),
    });
  } catch (e) {
    console.error("Failed to log alert to Supabase:", e.message);
  }

  // Webhook alert (if configured)
  if (ALERT_WEBHOOK) {
    try {
      await fetch(ALERT_WEBHOOK, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text: message }),
      });
    } catch (e) {
      console.error("Webhook alert failed:", e.message);
    }
  }
}

function getUptime() {
  const started = new Date(serverMetrics.startedAt).getTime();
  const diff = Date.now() - started;
  const hours = Math.floor(diff / 3600000);
  const mins = Math.floor((diff % 3600000) / 60000);
  return `${hours}h ${mins}m`;
}

function getTopTools(n) {
  return Object.entries(serverMetrics.toolCalls)
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([name, count]) => `${name}(${count})`)
    .join(", ") || "none";
}

const ALL_CONTENT_TYPES = ["all", "blog", "research_note", "research_paper", "x_post", "linkedin_post", "press_citation", "podcast"];

const ANALYST_MAP = {
  1: "Patrick Moorhead",
  2: "Matt Kimball",
  3: "Robert Kramer",
  4: "Jason Andersen",
  5: "Melody Brue",
  6: "Anshel Sag",
  7: "Paul Smith-Goodson",
  8: "Bill Curtis",
  9: "Will Townsend",
  10: "Moor Insights",
  11: "Paula Moorhead",
};

const ANALYST_NAME_TO_ID = Object.fromEntries(
  Object.entries(ANALYST_MAP).map(([id, name]) => [name.toLowerCase(), parseInt(id)])
);

// ── Supabase helpers ──
async function supabaseRPC(functionName, params = {}) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${functionName}`, {
    method: "POST",
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(params),
  });
  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`RPC ${functionName} failed: ${res.status} - ${errText}`);
  }
  return res.json();
}

async function supabaseQuery(table, params = "") {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${params}`, {
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
    },
  });
  if (!res.ok) throw new Error(`Query failed: ${res.status}`);
  return res.json();
}

async function callEdgeFunction(question, { model, analyst_id } = {}) {
  const body = { question };
  if (model) body.model = model;
  if (analyst_id) body.analyst_id = analyst_id;

  const res = await fetch(EDGE_FUNCTION_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${ANON_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Edge function failed: ${res.status} - ${errText}`);
  }
  return res.json();
}

// ── Create MCP Server ──
function createServer() {
  const server = new McpServer({
    name: "MIS Content Repository",
    version: "2.1.0",
  });

  // ── Tool: ask_question ──
  server.tool(
    "ask_question",
    "Ask a question about technology topics and get an AI-synthesized answer grounded in Moor Insights & Strategy analyst content. Uses hybrid semantic + keyword search across 69,000+ pieces of analyst content including blogs, research papers, podcasts (Six Five, Datacenter, Hot Desk, Game Time Tech, G2 on 5G, Enterprise Apps, MIS Insider), X posts, LinkedIn posts, and press citations. Best for getting a quick synthesized answer with citations. For raw source material to use in content creation, use search_content or get_content_by_topic instead.",
    {
      question: z.string().describe("The question to ask about technology topics, companies, or industry trends"),
      model: z.enum(["sonnet", "opus", "haiku"]).default("sonnet").describe("AI model to use: sonnet (default, balanced), opus (highest quality), haiku (fastest)"),
      analyst: z.string().optional().describe("Filter to a specific analyst's content (e.g. 'Patrick Moorhead', 'Anshel Sag')"),
    },
    async ({ question, model, analyst }) => {
      try {
        const opts = {};
        if (model && model !== "sonnet") opts.model = model;
        if (analyst) {
          const aid = ANALYST_NAME_TO_ID[analyst.toLowerCase()];
          if (aid) opts.analyst_id = aid;
        }

        const result = await callEdgeFunction(question, opts);

        let text = result.answer || "No answer returned.";
        if (result.sources && result.sources.length > 0) {
          text += "\n\nSources:\n";
          result.sources.forEach((s, i) => {
            text += `${i + 1}. "${s.title}" (${s.content_type}, ${s.date})${s.url ? ` — ${s.url}` : ""}\n`;
          });
        }
        text += `\n[Model: ${result.model || model}, Search: ${result.search_mode || "unknown"}]`;

        return {
          content: [{ type: "text", text }],
        };
      } catch (e) {
        return {
          content: [{ type: "text", text: `Error: ${e.message}` }],
          isError: true,
        };
      }
    }
  );

  // ── Tool: search_content ──
  server.tool(
    "search_content",
    "Search the MIS content repository using hybrid semantic + keyword search. Returns relevant analyst content ranked by relevance with excerpts. Covers blogs, research notes, research papers, podcasts, X posts, LinkedIn posts, and press citations. Use this to find specific pieces of content, then use get_full_content to retrieve the complete text of items you want to reference.",
    {
      query: z.string().describe("Search query — can be natural language (e.g. 'Intel foundry strategy', 'NVIDIA AI competitive moat')"),
      content_type: z.enum(ALL_CONTENT_TYPES).default("all").describe("Filter by content type: blog, research_note, research_paper, x_post, linkedin_post, press_citation, podcast, or all"),
      analyst: z.string().optional().describe("Filter by analyst name (e.g. 'Patrick Moorhead', 'Anshel Sag')"),
      limit: z.number().min(1).max(200).default(10).describe("Number of results to return (max 200)"),
      offset: z.number().min(0).default(0).describe("Offset for pagination — skip this many results (use with limit to page through large result sets)"),
    },
    async ({ query, content_type, analyst, limit, offset }) => {
      try {
        const opts = {};
        if (analyst) {
          const aid = ANALYST_NAME_TO_ID[analyst.toLowerCase()];
          if (aid) opts.analyst_id = aid;
        }

        const result = await callEdgeFunction(query, opts);

        let sources = result.sources || [];

        if (content_type && content_type !== "all") {
          sources = sources.filter((s) => s.content_type === content_type);
        }

        sources = sources.slice(offset, offset + limit);

        if (sources.length === 0) {
          return { content: [{ type: "text", text: "No results found." }] };
        }

        const formatted = sources.map((r, i) => {
          return [
            `## Result ${i + 1} (ID: ${r.content_id || "unknown"})`,
            `**Title:** ${r.title || "(untitled)"}`,
            `**Type:** ${r.content_type || "unknown"}`,
            `**Date:** ${r.date || "unknown"}`,
            r.url ? `**URL:** ${r.url}` : "",
            r.score ? `**Relevance:** ${Number(r.score).toFixed(3)}` : "",
            "",
          ].filter(Boolean).join("\n");
        });

        return {
          content: [{ type: "text", text: `Found ${sources.length} results:\n\n${formatted.join("\n---\n\n")}\n\nTip: Use get_full_content with the content ID to retrieve the complete text of any item.` }],
        };
      } catch (e) {
        // Fallback: use full-text search via PostgREST
        try {
          let queryParams = `select=id,title,body_text,content_type,source_url,published_date,analyst_id,word_count&order=published_date.desc&limit=${limit}`;

          const tsQuery = query.split(/\s+/).filter(Boolean).join(" & ");
          queryParams += `&fts=fts.${encodeURIComponent(tsQuery)}`;

          if (content_type && content_type !== "all") {
            queryParams += `&content_type=eq.${content_type}`;
          }
          if (analyst) {
            const aid = ANALYST_NAME_TO_ID[analyst.toLowerCase()];
            if (aid) queryParams += `&analyst_id=eq.${aid}`;
          }

          const results = await supabaseQuery("content_items", queryParams);

          if (!results || results.length === 0) {
            return { content: [{ type: "text", text: `No results found. (Fallback search was used. Primary error: ${e.message})` }] };
          }

          const formatted = results.map((r, i) => {
            const analystName = ANALYST_MAP[r.analyst_id] || "Unknown";
            return [
              `## Result ${i + 1} (ID: ${r.id})`,
              `**Title:** ${r.title || "(untitled)"}`,
              `**Analyst:** ${analystName}`,
              `**Type:** ${r.content_type}`,
              `**Date:** ${r.published_date || "unknown"}`,
              r.source_url ? `**URL:** ${r.source_url}` : "",
              `**Excerpt:** ${(r.body_text || "").substring(0, 800)}${(r.body_text || "").length > 800 ? "..." : ""}`,
              "",
            ].filter(Boolean).join("\n");
          });

          return {
            content: [{ type: "text", text: `Found ${results.length} results (full-text search):\n\n${formatted.join("\n---\n\n")}` }],
          };
        } catch (fallbackErr) {
          return {
            content: [{ type: "text", text: `Error: ${e.message} (fallback also failed: ${fallbackErr.message})` }],
            isError: true,
          };
        }
      }
    }
  );

  // ── Tool: get_full_content ──
  server.tool(
    "get_full_content",
    "Retrieve the COMPLETE text of one or more content items by ID. Use this after search_content or get_content_by_topic to get the full analyst opinions, arguments, and quotes — not just excerpts. Essential for content creation where you need to accurately represent analyst positions. Returns the full body text, title, date, analyst name, content type, and source URL.",
    {
      content_ids: z.array(z.number()).min(1).max(10).describe("Array of content item IDs to retrieve (get IDs from search_content or get_content_by_topic results)"),
    },
    async ({ content_ids }) => {
      try {
        const idsParam = content_ids.join(",");
        const results = await supabaseQuery(
          "content_items",
          `id=in.(${idsParam})&select=id,title,body_text,content_type,source_url,published_date,analyst_id,word_count,engagement_metrics`
        );

        if (!results || results.length === 0) {
          return { content: [{ type: "text", text: "No content found for the given IDs." }] };
        }

        const formatted = results.map((r) => {
          const analystName = ANALYST_MAP[r.analyst_id] || "Unknown";
          const meta = r.engagement_metrics || {};
          const podcastInfo = meta.podcast_name ? `\n**Podcast:** ${meta.podcast_name}` : "";
          const speakerInfo = meta.speaker ? `\n**Speaker:** ${meta.speaker}` : "";

          return [
            `## ${r.title || "(untitled)"} (ID: ${r.id})`,
            `**Analyst:** ${analystName} | **Type:** ${r.content_type} | **Date:** ${r.published_date || "unknown"} | **Words:** ${r.word_count || 0}`,
            r.source_url ? `**URL:** ${r.source_url}` : "",
            podcastInfo,
            speakerInfo,
            "",
            r.body_text || "(no content)",
            "",
          ].filter(Boolean).join("\n");
        });

        return {
          content: [{ type: "text", text: formatted.join("\n\n---\n\n") }],
        };
      } catch (e) {
        return {
          content: [{ type: "text", text: `Error: ${e.message}` }],
          isError: true,
        };
      }
    }
  );

  // ── Tool: get_content_by_topic ──
  server.tool(
    "get_content_by_topic",
    "Get ALL MIS analyst content on a specific topic, sorted chronologically. Returns content IDs, titles, dates, types, and excerpts. Use this when creating blogs, research reports, or show notes to understand the full timeline of MIS analyst opinions on a topic — how views evolved over time. Follow up with get_full_content on the most relevant item IDs to get complete text.",
    {
      topic: z.string().describe("Topic to search for (e.g. 'Intel foundry', 'AMD EPYC', 'Qualcomm Snapdragon X Elite', 'cloud infrastructure AI')"),
      analyst: z.string().optional().describe("Filter by analyst name"),
      content_types: z.array(z.enum(ALL_CONTENT_TYPES.filter(t => t !== "all"))).optional().describe("Filter to specific content types (e.g. ['blog', 'research_note', 'podcast'] for long-form only)"),
      date_from: z.string().optional().describe("Only include content from this date onward (YYYY-MM-DD format)"),
      date_to: z.string().optional().describe("Only include content up to this date (YYYY-MM-DD format)"),
      limit: z.number().min(1).max(200).default(25).describe("Maximum number of results (default 25, max 200)"),
      offset: z.number().min(0).default(0).describe("Offset for pagination — skip this many results (use with limit to page through large result sets)"),
    },
    async ({ topic, analyst, content_types, date_from, date_to, limit, offset }) => {
      try {
        // Full-text search with filters, sorted chronologically
        const tsQuery = topic.split(/\s+/).filter(Boolean).join(" & ");
        let queryParams = `select=id,title,body_text,content_type,source_url,published_date,analyst_id,word_count&order=published_date.desc&limit=${limit}&offset=${offset}`;
        queryParams += `&fts=fts.${encodeURIComponent(tsQuery)}`;

        if (analyst) {
          const aid = ANALYST_NAME_TO_ID[analyst.toLowerCase()];
          if (aid) queryParams += `&analyst_id=eq.${aid}`;
        }

        if (content_types && content_types.length > 0) {
          queryParams += `&content_type=in.(${content_types.join(",")})`;
        }

        if (date_from) {
          queryParams += `&published_date=gte.${date_from}`;
        }
        if (date_to) {
          queryParams += `&published_date=lte.${date_to}`;
        }

        const results = await supabaseQuery("content_items", queryParams);

        if (!results || results.length === 0) {
          return { content: [{ type: "text", text: `No content found for topic: "${topic}". Try broader search terms.` }] };
        }

        // Group by year for chronological overview
        const byYear = {};
        results.forEach((r) => {
          const year = (r.published_date || "unknown").substring(0, 4);
          if (!byYear[year]) byYear[year] = [];
          byYear[year].push(r);
        });

        let text = `Found ${results.length} items on "${topic}":\n\n`;

        for (const [year, items] of Object.entries(byYear).sort((a, b) => b[0].localeCompare(a[0]))) {
          text += `### ${year} (${items.length} items)\n\n`;
          items.forEach((r) => {
            const analystName = ANALYST_MAP[r.analyst_id] || "Unknown";
            const excerpt = (r.body_text || "").substring(0, 300);
            text += `- **ID ${r.id}** | ${r.published_date || "?"} | ${r.content_type} | ${analystName}\n`;
            text += `  "${r.title || "(untitled)"}"\n`;
            text += `  ${excerpt}${excerpt.length >= 300 ? "..." : ""}\n\n`;
          });
        }

        text += `\nTo get the full text of specific items, use get_full_content with the IDs listed above.`;

        return {
          content: [{ type: "text", text }],
        };
      } catch (e) {
        return {
          content: [{ type: "text", text: `Error: ${e.message}` }],
          isError: true,
        };
      }
    }
  );

  // ── Tool: get_analyst_content ──
  server.tool(
    "get_analyst_content",
    "Get recent content from a specific MIS analyst. Returns their latest posts, blogs, research, podcasts, and press citations with excerpts. Use get_full_content with item IDs to retrieve complete text.",
    {
      analyst: z.string().describe("Analyst name (e.g. 'Patrick Moorhead', 'Matt Kimball', 'Anshel Sag', 'Jason Andersen', 'Melody Brue', 'Robert Kramer', 'Paul Smith-Goodson', 'Bill Curtis', 'Will Townsend', 'Moor Insights')"),
      content_type: z.enum(ALL_CONTENT_TYPES).default("all").describe("Filter by content type"),
      limit: z.number().min(1).max(200).default(10).describe("Number of results (max 200)"),
      offset: z.number().min(0).default(0).describe("Offset for pagination — skip this many results (use with limit to page through all content)"),
    },
    async ({ analyst, content_type, limit, offset }) => {
      try {
        const aid = ANALYST_NAME_TO_ID[analyst.toLowerCase()];
        if (!aid) {
          return {
            content: [{
              type: "text",
              text: `Unknown analyst: "${analyst}". Available: ${Object.values(ANALYST_MAP).join(", ")}`,
            }],
          };
        }

        let queryParams = `analyst_id=eq.${aid}&order=published_date.desc&limit=${limit}&offset=${offset}&select=id,title,body_text,content_type,source_url,published_date,word_count`;
        if (content_type && content_type !== "all") {
          queryParams += `&content_type=eq.${content_type}`;
        }

        const results = await supabaseQuery("content_items", queryParams);

        if (!results || results.length === 0) {
          return { content: [{ type: "text", text: `No content found for ${analyst}.` }] };
        }

        const formatted = results.map((r, i) => {
          return [
            `## ${i + 1}. ${r.title || "(untitled)"} (ID: ${r.id})`,
            `**Type:** ${r.content_type} | **Date:** ${r.published_date || "unknown"} | **Words:** ${r.word_count || 0}`,
            r.source_url ? `**URL:** ${r.source_url}` : "",
            `${(r.body_text || "").substring(0, 600)}${(r.body_text || "").length > 600 ? "..." : ""}`,
            "",
          ].filter(Boolean).join("\n");
        });

        return {
          content: [{ type: "text", text: `Recent content from ${analyst}:\n\n${formatted.join("\n---\n\n")}\n\nUse get_full_content with IDs to retrieve complete text.` }],
        };
      } catch (e) {
        return {
          content: [{ type: "text", text: `Error: ${e.message}` }],
          isError: true,
        };
      }
    }
  );

  // ── Tool: get_stats ──
  server.tool(
    "get_stats",
    "Get statistics about the MIS content repository — total content count, breakdown by type, by analyst, and recent activity.",
    {},
    async () => {
      try {
        const stats = await supabaseRPC("get_content_stats");

        const byType = (stats.by_type || [])
          .map((t) => `  ${t.content_type}: ${t.count.toLocaleString()}`)
          .join("\n");

        const byAnalyst = (stats.by_analyst || [])
          .map((a) => `  ${a.name}: ${a.count.toLocaleString()}`)
          .join("\n");

        const recentMonths = (stats.by_month || []).slice(0, 6)
          .map((m) => `  ${m.month}: ${m.count.toLocaleString()}`)
          .join("\n");

        const text = [
          `MIS Content Repository Stats`,
          ``,
          `Total items: ${stats.total_content?.toLocaleString() || "unknown"}`,
          `Total words: ${stats.total_words?.toLocaleString() || "unknown"}`,
          `Active analysts: ${stats.active_analysts || "unknown"}`,
          `This month: ${stats.this_month?.toLocaleString() || 0} new items`,
          ``,
          `By Content Type:`,
          byType,
          ``,
          `By Analyst:`,
          byAnalyst,
          ``,
          `Recent Activity (by month):`,
          recentMonths,
        ].join("\n");

        return { content: [{ type: "text", text }] };
      } catch (e) {
        return {
          content: [{ type: "text", text: `Error: ${e.message}` }],
          isError: true,
        };
      }
    }
  );

  // ── Tool: get_analyst_stats ──
  server.tool(
    "get_analyst_stats",
    "Get detailed stats for each MIS analyst — content counts, latest activity, and areas of focus.",
    {},
    async () => {
      try {
        const stats = await supabaseRPC("get_analyst_stats");

        const formatted = stats.map((a) => {
          return `${a.name} (${a.title || "Analyst"})\n  X: ${a.x_handle || "N/A"} | Items: ${a.content_count?.toLocaleString()} | Latest: ${a.latest_content_date || "N/A"}`;
        }).join("\n\n");

        return {
          content: [{ type: "text", text: `MIS Analyst Overview:\n\n${formatted}` }],
        };
      } catch (e) {
        return {
          content: [{ type: "text", text: `Error: ${e.message}` }],
          isError: true,
        };
      }
    }
  );

  return server;
}

// ── Express app with Streamable HTTP transport ──
const app = express();
const PORT = process.env.PORT || 5050;

app.use(cors({
  origin: true,
  methods: ["GET", "POST", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Accept", "mcp-session-id", "Authorization"],
  exposedHeaders: ["mcp-session-id"],
  credentials: true,
}));
app.use(express.json());

const transports = {};

app.post("/mcp", async (req, res) => {
  const startTime = Date.now();
  try {
    const sessionId = req.headers["mcp-session-id"];
    const userAgent = req.headers["user-agent"] || "unknown";
    const clientIp = req.headers["x-forwarded-for"]?.split(",")[0]?.trim() || req.socket.remoteAddress || "unknown";
    let transport;
    let isNewSession = false;

    if (sessionId && transports[sessionId]) {
      transport = transports[sessionId];
    } else {
      isNewSession = true;
      const server = createServer();
      transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => crypto.randomUUID(),
      });
      await server.connect(transport);

      transport.onclose = () => {
        const sid = transport.sessionId;
        if (sid && transports[sid]) {
          delete transports[sid];
          delete sessionMeta[sid];
        }
      };
    }

    await transport.handleRequest(req, res, req.body);

    if (transport.sessionId && !transports[transport.sessionId]) {
      transports[transport.sessionId] = transport;
    }

    // Track new session metadata
    if (isNewSession && transport.sessionId) {
      sessionMeta[transport.sessionId] = {
        connectedAt: new Date().toISOString(),
        lastActive: new Date().toISOString(),
        requestCount: 1,
        lastTool: null,
        userAgent: parseUserAgent(userAgent),
        ip: clientIp,
      };
    }

    // Detect tool name from request body (MCP JSON-RPC)
    let toolName = null;
    if (req.body?.method === "tools/call" && req.body?.params?.name) {
      toolName = req.body.params.name;
    }

    trackRequest(transport.sessionId, toolName, Date.now() - startTime);

  } catch (e) {
    console.error("MCP error:", e);
    trackRequest(null, null, Date.now() - startTime, true);
    if (!res.headersSent) {
      res.status(500).json({ error: e.message });
    }
  }
});

function parseUserAgent(ua) {
  if (!ua || ua === "unknown") return "Unknown";
  if (ua.includes("Claude") || ua.includes("claude") || ua.includes("anthropic")) return "Claude.ai";
  if (ua.includes("Cursor")) return "Cursor";
  if (ua.includes("Windsurf")) return "Windsurf";
  if (ua.includes("VSCode") || ua.includes("vscode")) return "VS Code";
  if (ua.includes("Copilot")) return "GitHub Copilot";
  if (ua.includes("Perplexity") || ua.includes("perplexity")) return "Perplexity";
  if (ua.includes("OpenAI") || ua.includes("ChatGPT")) return "ChatGPT";
  if (ua.includes("node-fetch") || ua.includes("Node")) return "Node.js Client";
  if (ua.includes("Python") || ua.includes("python")) return "Python Client";
  if (ua.includes("Mozilla") || ua.includes("Chrome") || ua.includes("Safari")) return "Browser";
  return ua.substring(0, 50);
}

app.get("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"];
  if (sessionId && transports[sessionId]) {
    const transport = transports[sessionId];
    await transport.handleRequest(req, res);
  } else {
    res.status(400).json({ error: "No session. Send a POST first." });
  }
});

app.delete("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"];
  if (sessionId && transports[sessionId]) {
    const transport = transports[sessionId];
    await transport.handleRequest(req, res);
    delete transports[sessionId];
    delete sessionMeta[sessionId];
  } else {
    res.status(400).json({ error: "No session found." });
  }
});

app.get("/health", (req, res) => {
  const activeSessions = Object.keys(transports).length;
  const rpm = serverMetrics.requestsLastMinute.length;
  const avgResponseTime = serverMetrics.responseTimes.length > 0
    ? Math.round(serverMetrics.responseTimes.reduce((a, b) => a + b, 0) / serverMetrics.responseTimes.length)
    : 0;
  const errorRate = serverMetrics.totalRequests > 0
    ? ((serverMetrics.totalErrors / serverMetrics.totalRequests) * 100).toFixed(1)
    : "0.0";

  res.json({
    status: "ok",
    name: "MIS Content Repository MCP Server",
    version: "2.2.0",
    tools: ["ask_question", "search_content", "get_full_content", "get_content_by_topic", "get_analyst_content", "get_stats", "get_analyst_stats"],
    uptime: getUptime(),
    sessions: {
      active: activeSessions,
      details: Object.entries(sessionMeta).map(([id, meta]) => ({
        id: id.substring(0, 8) + "...",
        client: meta.userAgent,
        connected_at: meta.connectedAt,
        last_active: meta.lastActive,
        requests: meta.requestCount,
        last_tool: meta.lastTool,
        ip: meta.ip,
      })),
    },
    metrics: {
      total_requests: serverMetrics.totalRequests,
      total_errors: serverMetrics.totalErrors,
      error_rate: errorRate + "%",
      requests_per_minute: rpm,
      avg_response_time_ms: avgResponseTime,
      tool_usage: serverMetrics.toolCalls,
    },
    thresholds: OVERLOAD_THRESHOLDS,
  });
});

// ── Detailed metrics endpoint ──
app.get("/metrics", (req, res) => {
  const activeSessions = Object.keys(transports).length;
  res.json({
    server: {
      version: "2.2.0",
      started_at: serverMetrics.startedAt,
      uptime: getUptime(),
    },
    sessions: {
      active: activeSessions,
      list: Object.entries(sessionMeta).map(([id, meta]) => ({
        session_id: id.substring(0, 8) + "...",
        client: meta.userAgent,
        ip: meta.ip,
        connected_at: meta.connectedAt,
        last_active: meta.lastActive,
        request_count: meta.requestCount,
        last_tool_used: meta.lastTool,
      })),
    },
    performance: {
      total_requests: serverMetrics.totalRequests,
      total_errors: serverMetrics.totalErrors,
      error_rate_percent: serverMetrics.totalRequests > 0
        ? ((serverMetrics.totalErrors / serverMetrics.totalRequests) * 100).toFixed(1)
        : "0.0",
      requests_per_minute: serverMetrics.requestsLastMinute.length,
      avg_response_time_ms: serverMetrics.responseTimes.length > 0
        ? Math.round(serverMetrics.responseTimes.reduce((a, b) => a + b, 0) / serverMetrics.responseTimes.length)
        : 0,
      p95_response_time_ms: serverMetrics.responseTimes.length > 0
        ? Math.round(serverMetrics.responseTimes.slice().sort((a, b) => a - b)[Math.floor(serverMetrics.responseTimes.length * 0.95)])
        : 0,
    },
    tool_usage: Object.entries(serverMetrics.toolCalls)
      .sort((a, b) => b[1] - a[1])
      .map(([name, count]) => ({ tool: name, calls: count })),
    alert_config: {
      thresholds: OVERLOAD_THRESHOLDS,
      cooldown_minutes: ALERT_COOLDOWN_MS / 60000,
      last_alert: lastAlertTime ? new Date(lastAlertTime).toISOString() : null,
    },
  });
});

app.listen(PORT, () => {
  console.log(`MIS MCP Server v2.2.0 running on port ${PORT}`);
  console.log(`MCP endpoint: http://localhost:${PORT}/mcp`);
  console.log(`Health check: http://localhost:${PORT}/health`);
  console.log(`Metrics:      http://localhost:${PORT}/metrics`);
  console.log(`Alert email:  ${ALERT_EMAIL}`);
});
