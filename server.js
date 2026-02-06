import express from "express";
import cors from "cors";
import dotenv from "dotenv";

import chatRoute from "./routes/chat.js";
import scanRoute from "./routes/scan.js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

app.get("/", (req, res) => {
  res.send("🧠 Kyra Brain is alive");
});

app.use("/chat", chatRoute);
app.use("/scan", scanRoute);

const PORT = process.env.PORT || 10000;
app.listen(PORT, () =>
  console.log("🔥 Kyra Brain running on port", PORT)
);
