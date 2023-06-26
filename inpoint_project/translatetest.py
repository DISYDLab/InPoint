from deep_translator import GoogleTranslator


translated = GoogleTranslator(source='auto', target='el').translate("keep it up, you are awesome")
print(translated)
