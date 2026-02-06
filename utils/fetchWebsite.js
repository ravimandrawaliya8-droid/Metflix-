import axios from "axios";
import * as cheerio from "cheerio";

export default async function fetchWebsite(url) {
  const { data } = await axios.get(url, { timeout: 10000 });

  const $ = cheerio.load(data);
  const text = $("body").text().replace(/\s+/g, " ").slice(0, 4000);

  return text;
}
