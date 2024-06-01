import unicodedata

word = input("Ingrese una palabra: ")

def vector_build(word):
    # Convertir la palabra a minúsculas
    word = word.lower()
    # Reemplazar espacios por "_"
    word = word.replace(" ", "_")
    # Quitar acentos y caracteres especiales
    word = ''.join((c for c in unicodedata.normalize('NFD', word) if unicodedata.category(c) != 'Mn'))
    # Quitar todo lo que no sea letra o "_"
    word = ''.join(c for c in word if c.isalnum() or c == "_")
    return word

print(vector_build(word))