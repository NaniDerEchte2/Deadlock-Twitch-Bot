import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import AffiliatePortal from "@/pages/AffiliatePortal";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <AffiliatePortal />
  </StrictMode>,
);
