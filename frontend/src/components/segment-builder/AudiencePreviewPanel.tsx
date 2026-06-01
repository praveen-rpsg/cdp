/**
 * AudiencePreviewPanel — lists sample customers matching the current segment
 * and lets the user open any one of them in the Customer Single View.
 */

import React from "react";
import { useSegmentStore } from "../../store/segmentStore";

const AudiencePreviewPanel: React.FC = () => {
  const {
    previewProfiles,
    isPreviewing,
    previewAudience,
    selectedBrandCode,
    setPendingSingleView,
    setPendingCorporateView,
  } = useSegmentStore();

  const isCorporate = selectedBrandCode === "corporate";

  const openSingleView = (mobile: string, customerId: string) => {
    // Hand off the customer via the store, then switch the top-level view.
    if (isCorporate) {
      // Corporate preview rows carry r1_id in customer_id.
      setPendingCorporateView({ mobile, r1_id: customerId || null });
      window.dispatchEvent(new CustomEvent("csv:navigate", { detail: { target: "corporate_view" } }));
    } else {
      if (!mobile) return;
      setPendingSingleView({ mobile, brand: selectedBrandCode });
      window.dispatchEvent(new CustomEvent("csv:navigate", { detail: { target: "single_view" } }));
    }
  };

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
          Customer Preview
        </h3>
        <button
          onClick={previewAudience}
          disabled={isPreviewing || !selectedBrandCode}
          className="px-2.5 py-1 text-xs bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-40 active:scale-95 transition"
        >
          {isPreviewing ? "Loading…" : "Preview Customers"}
        </button>
      </div>

      {previewProfiles.length === 0 && !isPreviewing && (
        <p className="text-xs text-gray-400 leading-relaxed">
          Click "Preview Customers" to list matching profiles. Then open any
          customer's 360° single view.
        </p>
      )}

      {previewProfiles.length > 0 && (
        <div className="space-y-1.5 max-h-96 overflow-y-auto pr-0.5">
          {previewProfiles.map((p, i) => {
            const mobile = p.mobile || p.canonical_mobile || "";
            const customerId = p.customer_id || "";
            // Corporate can open by r1_id even without a mobile.
            const canOpen = isCorporate ? !!(mobile || customerId) : !!mobile;
            return (
              <div
                key={customerId || i}
                className="flex items-center justify-between gap-2 px-2.5 py-2 bg-gray-50 rounded-lg border border-transparent hover:border-indigo-100"
              >
                <div className="min-w-0">
                  <div className="text-xs font-medium text-gray-700 truncate">
                    {p.name || "Unnamed"}
                  </div>
                  <div className="text-[10px] text-gray-400 truncate">
                    {mobile || (isCorporate ? customerId : "no mobile")}{p.city ? ` · ${p.city}` : ""}
                  </div>
                </div>
                <button
                  onClick={() => openSingleView(mobile, customerId)}
                  disabled={!canOpen}
                  className="flex-shrink-0 px-2 py-1 text-[11px] font-medium text-indigo-600 hover:bg-indigo-50 rounded-md disabled:opacity-30 transition"
                  title={canOpen ? "Open Single View" : "No identifier"}
                >
                  View →
                </button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

export default AudiencePreviewPanel;
