import React, { useEffect } from "react";
import { SegmentBuilder } from "./components/segment-builder/SegmentBuilder";
import { useSegmentStore } from "./store/segmentStore";
import type { Brand } from "./types/segment";

function App() {
  const { setBrands, fetchCatalog, catalogLoaded } = useSegmentStore();

  useEffect(() => {
    // Fetch brands from the API so brand codes are always in sync with the backend.
    // Previously this used a hardcoded list where NBL had code "nbl" instead of
    // "natures_basket", causing PgCompiler to silently fall back to Spencer's schemas.
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
        // Fallback if the API is unreachable at startup
        setBrands([
          { id: "1", code: "spencers",       name: "Spencers",        channels: ["b2c", "d2c", "ecom"], business_model: "retail",   is_active: true },
          { id: "2", code: "fmcg",            name: "FMCG",            channels: ["d2c", "b2b"],          business_model: "fmcg",     is_active: true },
          { id: "3", code: "power_cesc",      name: "Power CESC",      channels: ["b2c", "b2b"],          business_model: "utility",  is_active: true },
          { id: "4", code: "natures_basket",  name: "Nature's Basket", channels: ["b2c", "ecom"],         business_model: "grocery",  is_active: true },
        ]);
      });

    // Initial load of attribute catalog
    if (!catalogLoaded) {
      fetchCatalog();
    }
  }, []);

  return <SegmentBuilder />;
}

export default App;
