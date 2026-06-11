/**
 * Type-ahead customer search input. Fetches matches (partial mobile / id / name)
 * from /api/v1/customers/search and shows a dropdown. Picking one fires onPick.
 */
import React, { useEffect, useRef, useState } from "react";

interface Picked { id: string; mobile: string | null; name: string | null }

interface Props {
  brandCode: string;
  value: string;
  onChange: (v: string) => void;
  onPick: (p: Picked) => void;
  onEnter?: () => void;
  placeholder?: string;
  className?: string;
}

export const CustomerAutocomplete: React.FC<Props> = ({
  brandCode, value, onChange, onPick, onEnter, placeholder, className,
}) => {
  const [results, setResults] = useState<Picked[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  // Only user keystrokes should trigger the suggestion fetch — programmatic
  // value changes (deep-links from preview rows, picking a suggestion) must not.
  const userTyped = useRef(false);
  const boxRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!userTyped.current) { setOpen(false); return; }
    userTyped.current = false;
    const q = value.trim();
    if (q.length < 3) { setResults([]); setOpen(false); return; }
    const t = setTimeout(async () => {
      setLoading(true);
      try {
        const r = await fetch(
          `/api/v1/customers/search?brand_code=${encodeURIComponent(brandCode)}&q=${encodeURIComponent(q)}`
        );
        const d = await r.json();
        // Dedupe by customer id — the search can match the same profile
        // through multiple keys (mobile + id) and return it twice.
        const seen = new Set<string>();
        const unique = ((d.results || []) as Picked[]).filter((p) => {
          if (seen.has(p.id)) return false;
          seen.add(p.id);
          return true;
        });
        setResults(unique);
        setOpen(true);
      } catch {
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 250);
    return () => clearTimeout(t);
  }, [value, brandCode]);

  // Close on outside click
  useEffect(() => {
    const h = (e: MouseEvent) => {
      if (boxRef.current && !boxRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);

  const pick = (p: Picked) => {
    setOpen(false);
    setResults([]);
    onPick(p);
  };

  return (
    <div className="relative" ref={boxRef}>
      <input
        type="text"
        value={value}
        onChange={(e) => { userTyped.current = true; onChange(e.target.value); }}
        onFocus={() => { if (results.length) setOpen(true); }}
        onKeyDown={(e) => {
          if (e.key === "Enter") { setOpen(false); onEnter?.(); }
          if (e.key === "Escape") setOpen(false);
        }}
        placeholder={placeholder}
        className={className}
        autoComplete="off"
      />
      {open && (results.length > 0 || loading) && (
        <div className="absolute z-30 mt-1 w-full bg-white border border-gray-200 rounded-lg shadow-lg max-h-64 overflow-y-auto">
          {loading && <div className="px-3 py-2 text-xs text-gray-400">Searching…</div>}
          {!loading && results.map((r, i) => (
            <button
              key={`${r.id}-${i}`}
              onClick={() => pick(r)}
              className="w-full text-left px-3 py-2 hover:bg-indigo-50 border-b border-gray-50 last:border-0"
            >
              <div className="text-sm font-medium text-gray-800">
                {r.mobile || "(no mobile)"}{r.name ? ` · ${r.name}` : ""}
              </div>
              <div className="text-[10px] text-gray-400 font-mono truncate">{r.id}</div>
            </button>
          ))}
          {!loading && results.length === 0 && (
            <div className="px-3 py-2 text-xs text-gray-400">No matches</div>
          )}
        </div>
      )}
    </div>
  );
};
