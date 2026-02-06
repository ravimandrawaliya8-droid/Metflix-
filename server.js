import express from "express";
import cors from "cors";
import fetch from "node-fetch";
import dotenv from "dotenv";
import { createClient } from "@supabase/supabase-js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

/* ======================
   SUPABASE SETUP
====================== */
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

/* ======================
   KYRA SYSTEM PROMPT
====================== */
const KYRA_SYSTEM = `
You are Kyra, a friendly, intelligent AI assistant for Mohit.

Personality:
- Speak like a calm, supportive partner
- Explain decisions clearly
- Be honest and practical
- Never pretend to take actions
- Always guide, never execute

Abilities:
- Analyze website ideas, UI, UX, and code (when provided)
- Detect problems, risks, and improvements
- Help with decisions and planning
- Think fast and logically

Rules:
- Never access systems yourself
- Never claim real-world control
- If info is missing, ask Mohit clearly
`;

/* ======================
   MAIN API
====================== */
app.post("/kyra", async (req, res) => {
  try {
    const { message } = req.body;
    if (!message) {
      return res.status(400).json({ error: "No message provided" });
    }

    // --- AI CALL ---
    const aiResponse = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: KYRA_SYSTEM },
          { role: "user", content: message }
        ],
        temperature: 0.4
      })
    });

    const data = await aiResponse.json();
    const reply = data.choices?.[0]?.message?.content || "Kyra is thinking…";

    // --- STORE MEMORY (SUMMARY ONLY) ---
    await supabase.from("kyra_memory").insert([
      {
        user_name: "Mohit",
        summary: reply.slice(0, 500)
      }
    ]);

    res.json({ reply });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Kyra core failed" });
  }
});

/* ======================
   HEALTH CHECK
====================== */
app.get("/", (_, res) => {
  res.send("🧠 Kyra core is alive");
});

/* ======================
   START SERVER
====================== */
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("Kyra running on port", PORT);
});
