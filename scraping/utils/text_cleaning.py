import re
from bs4 import BeautifulSoup


def clean_and_extract_text(html_content: str | None) -> str:
    if not isinstance(html_content, str):
        return ""
    soup = BeautifulSoup(html_content, "html.parser")
    tags_to_delete = ["source", "img", "video", "picture", "a", "sup", "hr", "br", "style"]
    tags_to_unwrap = ["p", "li", "strong", "span", "h2", "ul", "i", "h1", "u", "div", "th", "td", "tr", "table", "ol",
                      "blockquote", "b", "h6"]
    for tag_name in tags_to_delete:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    for tag_name in tags_to_unwrap:
        for tag in soup.find_all(tag_name):
            tag.unwrap()
    text = soup.get_text(separator=' ', strip=False)
    normalized_text = re.sub(r"\s+", " ", text).strip()
    return normalized_text
