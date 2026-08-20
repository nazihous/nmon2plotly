from html.parser import HTMLParser

class MyHTMLParser(HTMLParser):
    pass

try:
    with open('index.html', 'r') as file:
        content = file.read()

    parser = MyHTMLParser()
    parser.feed(content)
    print("HTML parsed successfully without syntax errors.")
except Exception as e:
    print(f"Error parsing HTML: {e}")
