import React, { useEffect } from "react";
import { SegmentBuilder } from "./components/segment-builder/SegmentBuilder";
import { useSegmentStore } from "./store/segmentStore";
import type { Brand } from "./types/segment";

// Mock data for initial development — replaced by API calls in production
const MOCK_BRANDS: Brand[] = [
  { id: "1", code: "spencers", name: "Spencers", channels: ["b2c", "d2c", "ecom"], business_model: "retail", is_active: true },
  { id: "2", code: "fmcg", name: "FMCG", channels: ["d2c", "b2b"], business_model: "fmcg", is_active: true },
  { id: "3", code: "power_cesc", name: "Power CESC", channels: ["b2c", "b2b"], business_model: "utility", is_active: true },
  { id: "4", code: "nbl", name: "Nature's Basket", channels: ["b2c", "ecom"], business_model: "grocery", is_active: true },
];

function App() {
  const { setBrands, fetchCatalog, catalogLoaded } = useSegmentStore();

  useEffect(() => {
    setBrands(MOCK_BRANDS);

    // Initial load of attribute catalog
    if (!catalogLoaded) {
      fetchCatalog();
    }
  }, []);

  return <SegmentBuilder />;
}

export default App;
