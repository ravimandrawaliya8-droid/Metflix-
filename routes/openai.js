import OpenAI from "openai";

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

export async function askKyra(user, message, memory) {
  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    temperature: 0.4,
    messages: [
      {
        role: "system",
        content: `
You are KYRA, a highly intelligent AI assistant for ${user}.
Behave like Jarvis.
Explain reasoning step by step.
Act as mentor, strategist, and problem solver.
Never claim to take actions.
`
      },
      ...memory,
      { role: "user", content: message }
    ]
  });

  return completion.choices[0].message.content;
}
