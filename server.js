import crypto from "crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express from "express";
import { z } from "zod";

// ── Config ──
const SUPABASE_URL = "https://uhdshvkyfxzwabipkqtx.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVoZHNodmt5Znh6d2FiaXBrcXR4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzE2ODA0MCwiZXhwIjoyMDg4NzQ0MDQwfQ.FOxeK-GKmIGrAH3IxYGAx0fxHBh5obmjFc2Ytr3KA2s";
const EDGE_FUNCTION_URL = `${SUPABASE_URL}/functions/v1/ask`;
const ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVoZHNodmt5Znh6d2FiaXBrcXR4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMxNjgwNDAsImV4cCI6MjA4ODc0NDA0MH0.tKwF_H1n5OqDEKLyFsLjF77B40nvwibOTBmThmaeZng";

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
    version: "1.0.0",
  });

  // ── Tool: ask_question ──
  server.tool(
    "ask_question",
    "Ask a question about technology topics and get an AI-synthesized answer grounded in Moor Insights & Strategy analyst content. Uses hybrid semantic + keyword search with Claude to generate cited answers from 67,000+ pieces of analyst content including blog posts, research papers, X posts, LinkedIn posts, and press citations.",
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

        // Build response with answer + sources
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
    "Search the MIS content repository using hybrid semantic + keyword search. Returns relevant analyst content ranked by relevance. Covers blogs, research notes, research papers, X posts, LinkedIn posts, and press citations. For a full AI-synthesized answer, use ask_question instead.",
    {
      query: z.string().describe("Search query — can be natural language"),
      content_type: z.enum(["all", "blog", "research_note", "research_paper", "x_post", "linkedin_post", "press_citation"]).default("all").describe("Filter by content type"),
      analyst: z.string().optional().describe("Filter by analyst name (e.g. 'Patrick Moorhead', 'Anshel Sag')"),
      limit: z.number().min(1).max(50).default(10).describe("Number of results to return"),
    },
    async ({ query, content_type, analyst, limit }) => {
      try {
        // Use Edge Function for hybrid search (it does embedding + DB search internally)
        const opts = {};
        if (analyst) {
          const aid = ANALYST_NAME_TO_ID[analyst.toLowerCase()];
          if (aid) opts.analyst_id = aid;
        }

        const result = await callEdgeFunction(query, opts);

        let sources = result.sources || [];

        // Apply content_type filter client-side (Edge Function doesn't support it directly)
        if (content_type && content_type !== "all") {
          sources = sources.filter((s) => s.content_type === content_type);
        }

        // Apply limit
        sources = sources.slice(0, limit);

        if (sources.length === 0) {
          return { content: [{ type: "text", text: "No results found." }] };
        }

        const formatted = sources.map((r, i) => {
          return [
            `## Result ${i + 1}`,
            `**Title:** ${r.title || "(untitled)"}`,
            `**Type:** ${r.content_type || "unknown"}`,
            `**Date:** ${r.date || "unknown"}`,
            r.url ? `**URL:** ${r.url}` : "",
            r.score ? `**Relevance:** ${Number(r.score).toFixed(3)}` : "",
            "",
          ].filter(Boolean).join("\n");
        });

        return {
          content: [{ type: "text", text: `Found ${sources.length} results:\n\n${formatted.join("\n---\n\n")}` }],
        };
      } catch (e) {
        // Fallback: use full-text search via PostgREST
        try {
          let queryParams = `select=title,body_text,content_type,source_url,published_date,analyst_id,word_count&order=published_date.desc&limit=${limit}`;

          // Full-text search using the fts column
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
              `## Result ${i + 1}`,
              `**Title:** ${r.title || "(untitled)"}`,
              `**Analyst:** ${analystName}`,
              `**Type:** ${r.content_type}`,
              `**Date:** ${r.published_date || "unknown"}`,
              r.source_url ? `**URL:** ${r.source_url}` : "",
              `**Excerpt:** ${(r.body_text || "").substring(0, 500)}${(r.body_text || "").length > 500 ? "..." : ""}`,
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

  // ── Tool: get_analyst_content ──
  server.tool(
    "get_analyst_content",
    "Get recent content from a specific MIS analyst. Returns their latest posts, blogs, research, and press citations.",
    {
      analyst: z.string().describe("Analyst name (e.g. 'Patrick Moorhead', 'Matt Kimball', 'Anshel Sag', 'Jason Andersen', 'Melody Brue', 'Robert Kramer', 'Paul Smith-Goodson', 'Bill Curtis', 'Will Townsend')"),
      content_type: z.enum(["all", "blog", "research_note", "research_paper", "x_post", "linkedin_post", "press_citation"]).default("all").describe("Filter by content type"),
      limit: z.number().min(1).max(50).default(10).describe("Number of results"),
    },
    async ({ analyst, content_type, limit }) => {
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

        let queryParams = `analyst_id=eq.${aid}&order=published_date.desc&limit=${limit}&select=title,body_text,content_type,source_url,published_date,word_count`;
        if (content_type && content_type !== "all") {
          queryParams += `&content_type=eq.${content_type}`;
        }

        const results = await supabaseQuery("content_items", queryParams);

        if (!results || results.length === 0) {
          return { content: [{ type: "text", text: `No content found for ${analyst}.` }] };
        }

        const formatted = results.map((r, i) => {
          return [
            `## ${i + 1}. ${r.title || "(untitled)"}`,
            `**Type:** ${r.content_type} | **Date:** ${r.published_date || "unknown"} | **Words:** ${r.word_count || 0}`,
            r.source_url ? `**URL:** ${r.source_url}` : "",
            `${(r.body_text || "").substring(0, 400)}${(r.body_text || "").length > 400 ? "..." : ""}`,
            "",
          ].filter(Boolean).join("\n");
        });

        return {
          content: [{ type: "text", text: `Recent content from ${analyst}:\n\n${formatted.join("\n---\n\n")}` }],
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

app.use(express.json());

// Store transports by session
const transports = {};

app.post("/mcp", async (req, res) => {
  try {
    const sessionId = req.headers["mcp-session-id"];
    let transport;

    if (sessionId && transports[sessionId]) {
      transport = transports[sessionId];
    } else {
      const server = createServer();
      transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => crypto.randomUUID(),
      });
      await server.connect(transport);

      transport.onclose = () => {
        const sid = transport.sessionId;
        if (sid && transports[sid]) {
          delete transports[sid];
        }
      };
    }

    await transport.handleRequest(req, res, req.body);

    if (transport.sessionId && !transports[transport.sessionId]) {
      transports[transport.sessionId] = transport;
    }
  } catch (e) {
    console.error("MCP error:", e);
    if (!res.headersSent) {
      res.status(500).json({ error: e.message });
    }
  }
});

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
  } else {
    res.status(400).json({ error: "No session found." });
  }
});

// Health check
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    name: "MIS Content Repository MCP Server",
    tools: ["ask_question", "search_content", "get_analyst_content", "get_stats", "get_analyst_stats"],
    sessions: Object.keys(transports).length,
  });
});

app.listen(PORT, () => {
  console.log(`MIS MCP Server running on port ${PORT}`);
  console.log(`MCP endpoint: http://localhost:${PORT}/mcp`);
  console.log(`Health check: http://localhost:${PORT}/health`);
});
