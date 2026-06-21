import urllib.request
import re

html = urllib.request.urlopen('https://developers.google.com/youtube/v3/live/streaming-live-chat').read().decode('utf-8')

# The HTML might have tags inside the code block
from html.parser import HTMLParser

class CodeParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_code = False
        self.code_blocks = []
        self.current_block = []

    def handle_starttag(self, tag, attrs):
        if tag == 'code':
            self.in_code = True

    def handle_endtag(self, tag):
        if tag == 'code':
            self.in_code = False
            self.code_blocks.append("".join(self.current_block))
            self.current_block = []

    def handle_data(self, data):
        if self.in_code:
            self.current_block.append(data)

parser = CodeParser()
parser.feed(html)

for block in parser.code_blocks:
    if 'syntax = "proto2";' in block:
        with open('stream_list.proto', 'w', encoding='utf-8') as f:
            f.write(block)
        print("Successfully extracted stream_list.proto!")
        break
else:
    print("Failed to find proto block")
