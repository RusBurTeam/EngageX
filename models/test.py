from transformers import pipeline

print("🚀 Запускаем минимальную версию...")

try:
    generator = pipeline("text-generation", model="sberbank-ai/rugpt3small_based_on_gpt2")
    result = generator("Bitcoin:", max_length=50, num_return_sequences=1)
    print("✅ Успех!")
    print(result[0]['generated_text'])
except Exception as e:
    print(f"❌ Ошибка: {e}")
    print("💡 Попробуйте перезапустить с стабильным интернетом")