import OpenAI from "openai";

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

export async function askKyra(user, message, memory = []) {
  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content: `
You are KYRA, a friendly, intelligent AI assistant.
User name: ${user}
Behave like Jarvis.
Explain decisions clearly.
Think step by step.
Act as mentor + strategist.
Never claim real-world actions.
`
      },
      ...memory.map(m => ({ role: "user", content: m })),
      { role: "user", content: message }
    ],
    temperature: 0.4
  });

  return completion.choices[0].message.content;
}
