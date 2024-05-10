from transformers import AutoTokenizer

tokenizer = AutoTokenizer.from_pretrained("./trained_tokenizer")

print(f"{tokenizer=}")

text = ["Hello, World!"]

encoded = tokenizer.batch_encode_plus(text)

print(f"{encoded=}")
