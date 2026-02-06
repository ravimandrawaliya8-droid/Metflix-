import express from "express";
import { askKyra } from "../services/openai.js";
import { saveMemory, getMemory } from "../services/memory.js";

const router = express.Router();

router.post("/", async (req, res) => {
  try {
    const { user = "Mohit", message } = req.body;
    if (!message) return res.status(400).json({ error: "Message required" });

    const memory = await getMemory(user);
    const reply = await askKyra(user, message, memory);

    await saveMemory(user, message, reply);

    res.json({ reply });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "Kyra thinking failed" });
  }
});

export default router;
