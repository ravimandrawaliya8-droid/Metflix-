import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

export async function saveMemory(user, input, reply) {
  await supabase.from("memories").insert([
    { user, role: "user", content: input },
    { user, role: "assistant", content: reply }
  ]);
}

export async function getMemory(user) {
  const { data } = await supabase
    .from("memories")
    .select("role, content")
    .eq("user", user)
    .order("created_at", { ascending: false })
    .limit(8);

  return data ? data.reverse() : [];
}
