import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import { BotFaqPage } from "@/pages/BotFaqPage";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BotFaqPage />
  </StrictMode>,
);
