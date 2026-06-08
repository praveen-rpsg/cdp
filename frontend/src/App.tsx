import React, { useEffect, useState } from "react";
import { SegmentBuilder } from "./components/segment-builder/SegmentBuilder";
import { CustomerSingleView } from "./components/customer-view/CustomerSingleView";
import { CorporateSingleView } from "./components/customer-view/CorporateSingleView";
import { SavedSegments } from "./components/saved-segments/SavedSegments";
import { useSegmentStore } from "./store/segmentStore";
import type { Brand } from "./types/segment";

type View = "segmentation" | "saved_segments" | "single_view" | "corporate_view";

function App() {
  const { setBrands, fetchCatalog, catalogLoaded } = useSegmentStore();
  const [view, setView] = useState<View>("segmentation");

  useEffect(() => {
    // Fetch brands from the API so brand codes are always in sync with the backend.
    fetch("/api/v1/brands/")
      .then((r) => r.json())
      .then((data) => {
        const brands: Brand[] = (data.brands ?? []).map((b: any) => ({
          id: b.id,
          code: b.code,
          name: b.name,
          channels: b.channels ?? [],
          business_model: b.business_model ?? "",
          is_active: b.is_active ?? true,
        }));
        setBrands(brands);
      })
      .catch(() => {
        setBrands([
          { id: "1", code: "spencers",       name: "Spencers",        channels: ["b2c", "d2c", "ecom"], business_model: "retail",   is_active: true },
          { id: "4", code: "natures_basket",  name: "Nature's Basket", channels: ["b2c", "ecom"],         business_model: "grocery",  is_active: true },
        ]);
      });

    if (!catalogLoaded) {
      fetchCatalog();
    }
  }, []);

  // Allow other components (e.g. preview rows) to deep-link into a single view.
  useEffect(() => {
    const handler = (e: any) => {
      const target = e?.detail?.target as View | undefined;
      setView(target === "corporate_view" ? "corporate_view" : "single_view");
    };
    window.addEventListener("csv:navigate", handler);
    const toSeg = () => setView("segmentation");
    window.addEventListener("nav:segmentation", toSeg);
    return () => {
      window.removeEventListener("csv:navigate", handler);
      window.removeEventListener("nav:segmentation", toSeg);
    };
  }, []);

  const tabs: { key: View; label: string }[] = [
    { key: "segmentation", label: "Segmentation" },
    { key: "saved_segments", label: "Saved Segments" },
    { key: "single_view", label: "Customer Single View" },
    { key: "corporate_view", label: "Corporate Single View" },
  ];

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      {/* Global tab nav */}
      <div className="bg-white border-b border-gray-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 flex items-center gap-1 h-12">
          {tabs.map((t) => (
            <button
              key={t.key}
              onClick={() => setView(t.key)}
              className={`px-4 py-2 text-sm font-medium rounded-t-lg border-b-2 transition-colors ${
                view === t.key
                  ? "border-indigo-600 text-indigo-700"
                  : "border-transparent text-gray-500 hover:text-gray-700"
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {view === "segmentation" && <SegmentBuilder />}
      {view === "saved_segments" && <SavedSegments />}
      {view === "single_view" && <CustomerSingleView />}
      {view === "corporate_view" && <CorporateSingleView />}
    </div>
  );
}

export default App;
