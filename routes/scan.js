import express from "express";
import fetchWebsite from "../utils/fetchWebsite.js";
import { askKyra } from "../services/openai.js";

const router = express.Router();

router.post("/", async (req, res) => {
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: "URL required" });

  try {
    const siteText = await fetchWebsite(url);

    const analysis = await askKyra(
      "Mohit",
      `Analyze this website like a mentor.
List problems, UX issues, SEO risks, and improvements:\n\n${siteText}`,
      []
    );

    res.json({ analysis });
  } catch (e) {
    res.status(500).json({ error: "Website scan failed" });
  }
});

export default router;
