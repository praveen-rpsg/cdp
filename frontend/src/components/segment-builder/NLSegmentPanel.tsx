import React, { useState, useRef, useEffect } from "react";
import { useSegmentStore } from "../../store/segmentStore";

interface NLResult {
  nl_query: string;
  rules: any | null;
  explanation: string;
  sql: string | null;
  estimated_count: number | null;
  error?: string;
  suggestion?: string;
}

interface ChatMessage {
  role: "user" | "assistant" | "system";
  id: string;
  content: string;
  result?: NLResult;
  timestamp: Date;
}

const EXAMPLE_QUERIES = [
  "High spenders with more than 50000 total spend",
  "Customers who haven't purchased in 60 days",
  "STAR segment customers in Kolkata",
  "Weekend shoppers who use promotions",
  "Omni-channel customers with spend decile 9 or 10",
  "New first-time buyers from last 30 days",
  "Churned customers with high lifetime value",
  "Customers who accept SMS marketing in Hyderabad",
  "Top NOB decile promo lovers",
  "Online-only shoppers with more than 10 bills",
];

export const NLSegmentPanel: React.FC = () => {
  const { selectedBrandCode, loadRules, setSegmentName, setSegmentDescription } =
    useSegmentStore();

  const [query, setQuery] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      id: "welcome",
      role: "system",
      content:
        "Describe your target audience in plain English. I'll translate it into a segment definition using Spencer's 84 attributes across identity, demographics, transactions, lifecycle, channel, and more.",
      timestamp: new Date(),
    },
  ]);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);

  const chatEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    if (query.length < 3) {
      setSuggestions([]);
      setShowSuggestions(false);
      return;
    }
    const timer = setTimeout(async () => {
      try {
        const res = await fetch("/api/v1/segments/nl/suggest", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query }),
        });
        const data = await res.json();
        setSuggestions(data.suggestions || []);
        setShowSuggestions(data.suggestions?.length > 0);
      } catch {
        setSuggestions([]);
      }
    }, 300);
    return () => clearTimeout(timer);
  }, [query]);

  const handleSubmit = async (queryText?: string) => {
    const q = (queryText || query).trim();
    if (!q || isLoading) return;

    setShowSuggestions(false);

    const userMsg: ChatMessage = {
      id: `user_${Date.now()}`,
      role: "user",
      content: q,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMsg]);
    setQuery("");
    setIsLoading(true);

    try {
      const res = await fetch("/api/v1/segments/nl/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: q,
          brand_code: selectedBrandCode || "spencers",
          execute: true,
        }),
      });
      const result: NLResult = await res.json();

      const assistantMsg: ChatMessage = {
        id: `ai_${Date.now()}`,
        role: "assistant",
        content: result.error
          ? `I couldn't process that query: ${result.error}${result.suggestion ? `\n\n${result.suggestion}` : ""}`
          : result.explanation || "Here are the results:",
        result: result.error ? undefined : result,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch {
      const errMsg: ChatMessage = {
        id: `err_${Date.now()}`,
        role: "assistant",
        content: "Failed to connect to the API. Please check that the backend is running.",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errMsg]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const handleLoadToBuilder = (result: NLResult) => {
    if (!result.rules?.root) return;

    const addIds = (node: any): any => {
      const id = `cond_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
      if (node.type === "group" || node.conditions) {
        return {
          ...node,
          type: "group",
          id,
          conditions: (node.conditions || []).map(addIds),
        };
      }
      return { ...node, id };
    };

    const rulesWithIds = addIds(result.rules.root);
    loadRules(rulesWithIds);
    setSegmentName(`NL: ${result.nl_query.slice(0, 50)}`);
    setSegmentDescription(result.explanation || result.nl_query);
  };

  return (
    <div className="flex flex-col h-full">
      {/* Chat history */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.map((msg) => (
          <div
            key={msg.id}
            className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
          >
            <div
              className={`max-w-[85%] rounded-lg px-4 py-3 ${
                msg.role === "user"
                  ? "bg-indigo-600 text-white"
                  : msg.role === "system"
                  ? "bg-gray-100 text-gray-600 text-sm italic"
                  : "bg-white border border-gray-200 text-gray-800"
              }`}
            >
              <p className="whitespace-pre-wrap">{msg.content}</p>

              {msg.result && (
                <div className="mt-3 space-y-3">
                  {msg.result.estimated_count !== null && (
                    <div className="flex items-center gap-2 bg-indigo-50 rounded-md px-3 py-2">
                      <span className="text-2xl font-bold text-indigo-700">
                        {msg.result.estimated_count.toLocaleString()}
                      </span>
                      <span className="text-sm text-indigo-500">matching profiles</span>
                    </div>
                  )}

                  {msg.result.sql && (
                    <details className="text-xs">
                      <summary className="cursor-pointer text-gray-400 hover:text-gray-600">
                        View SQL
                      </summary>
                      <pre className="mt-1 p-2 bg-gray-900 text-green-400 rounded text-[10px] overflow-x-auto whitespace-pre-wrap">
                        {msg.result.sql}
                      </pre>
                    </details>
                  )}

                  <div className="flex gap-2">
                    <button
                      onClick={() => handleLoadToBuilder(msg.result!)}
                      className="px-3 py-1.5 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700 transition"
                    >
                      Load to Visual Builder
                    </button>
                    <button
                      onClick={() => {
                        if (!msg.result?.sql) return;
                        if (navigator.clipboard) {
                          navigator.clipboard.writeText(msg.result.sql);
                        } else {
                          const ta = document.createElement("textarea");
                          ta.value = msg.result.sql;
                          ta.style.position = "fixed";
                          ta.style.opacity = "0";
                          document.body.appendChild(ta);
                          ta.select();
                          document.execCommand("copy");
                          document.body.removeChild(ta);
                        }
                      }}
                      className="px-3 py-1.5 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200 transition"
                    >
                      Copy SQL
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        ))}

        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-white border border-gray-200 rounded-lg px-4 py-3 text-gray-500 text-sm">
              <div className="flex items-center gap-2">
                <div className="flex gap-1">
                  <span className="w-2 h-2 bg-indigo-400 rounded-full animate-bounce [animation-delay:0ms]" />
                  <span className="w-2 h-2 bg-indigo-400 rounded-full animate-bounce [animation-delay:150ms]" />
                  <span className="w-2 h-2 bg-indigo-400 rounded-full animate-bounce [animation-delay:300ms]" />
                </div>
                Translating your query into segment rules...
              </div>
            </div>
          </div>
        )}

        <div ref={chatEndRef} />
      </div>

      {/* Example queries — shown only before first user message */}
      {messages.length <= 1 && (
        <div className="px-4 pb-2">
          <p className="text-xs text-gray-400 mb-2">Try one of these:</p>
          <div className="flex flex-wrap gap-2">
            {EXAMPLE_QUERIES.slice(0, 6).map((eq, i) => (
              <button
                key={i}
                onClick={() => {
                  setQuery(eq);
                  handleSubmit(eq);
                }}
                className="px-3 py-1.5 text-xs bg-gray-100 text-gray-600 rounded-full hover:bg-indigo-50 hover:text-indigo-700 transition"
              >
                {eq}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Input area */}
      <div className="border-t border-gray-200 bg-white p-4 relative">
        {showSuggestions && suggestions.length > 0 && (
          <div className="absolute bottom-full left-4 right-4 mb-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-48 overflow-y-auto z-10">
            {suggestions.map((s, i) => (
              <button
                key={i}
                onClick={() => {
                  setQuery(s);
                  setShowSuggestions(false);
                  inputRef.current?.focus();
                }}
                className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-indigo-50 hover:text-indigo-700 transition"
              >
                {s}
              </button>
            ))}
          </div>
        )}

        <div className="flex gap-3 items-end">
          <textarea
            ref={inputRef}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Describe your target audience... e.g. 'High spenders who haven't bought in 60 days in Kolkata'"
            rows={2}
            className="flex-1 resize-none border border-gray-300 rounded-lg px-4 py-3 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
          />
          <button
            onClick={() => handleSubmit()}
            disabled={isLoading || !query.trim()}
            className="px-6 py-3 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition font-medium text-sm whitespace-nowrap"
          >
            {isLoading ? "..." : "Ask"}
          </button>
        </div>
        <p className="text-[10px] text-gray-400 mt-1.5 text-center">
          Powered by Claude &middot; Grounded in Spencer's semantic data dictionary (84 attributes)
        </p>
      </div>
    </div>
  );
};
