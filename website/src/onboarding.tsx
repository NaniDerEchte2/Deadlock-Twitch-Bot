import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import { StreamerOnboardingPage } from "@/pages/StreamerOnboardingPage";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <StreamerOnboardingPage />
  </StrictMode>,
);
