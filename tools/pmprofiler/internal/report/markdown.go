package report

// Minimal markdown renderer for the embedded findings narrative. Supports the
// subset an engineering report actually uses: #/##/### headings, paragraphs,
// unordered and ordered lists, **bold**, `code`, fenced code blocks, and
// tables. Everything is HTML-escaped first; no raw HTML passthrough.

import (
	"fmt"
	"html"
	"html/template"
	"regexp"
	"strings"
)

var (
	boldRe = regexp.MustCompile(`\*\*([^*]+)\*\*`)
	codeRe = regexp.MustCompile("`([^`]+)`")
)

func inline(s string) string {
	s = html.EscapeString(s)
	s = boldRe.ReplaceAllString(s, "<strong>$1</strong>")
	s = codeRe.ReplaceAllString(s, "<code>$1</code>")
	return s
}

func mdToHTML(md string) template.HTML {
	if strings.TrimSpace(md) == "" {
		return ""
	}
	var b strings.Builder
	lines := strings.Split(md, "\n")
	var list string // "ul", "ol" or ""
	var para []string
	inCode := false

	closeList := func() {
		if list != "" {
			fmt.Fprintf(&b, "</%s>\n", list)
			list = ""
		}
	}
	flushPara := func() {
		if len(para) > 0 {
			b.WriteString("<p>" + inline(strings.Join(para, " ")) + "</p>\n")
			para = nil
		}
	}

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "```"):
			flushPara()
			closeList()
			if !inCode {
				b.WriteString("<pre><code>")
			} else {
				b.WriteString("</code></pre>\n")
			}
			inCode = !inCode
		case inCode:
			b.WriteString(html.EscapeString(line) + "\n")
		case trimmed == "":
			flushPara()
			closeList()
		case strings.HasPrefix(trimmed, "|") && strings.HasSuffix(trimmed, "|"):
			flushPara()
			closeList()
			// gather the whole table
			var rows [][]string
			for ; i < len(lines); i++ {
				t := strings.TrimSpace(lines[i])
				if !strings.HasPrefix(t, "|") {
					i--
					break
				}
				if strings.Trim(t, "|-: ") == "" { // separator row
					continue
				}
				cells := strings.Split(strings.Trim(t, "|"), "|")
				for j := range cells {
					cells[j] = strings.TrimSpace(cells[j])
				}
				rows = append(rows, cells)
			}
			if len(rows) > 0 {
				b.WriteString("<table><thead><tr>")
				for _, c := range rows[0] {
					b.WriteString("<th>" + inline(c) + "</th>")
				}
				b.WriteString("</tr></thead><tbody>")
				for _, r := range rows[1:] {
					b.WriteString("<tr>")
					for _, c := range r {
						b.WriteString("<td>" + inline(c) + "</td>")
					}
					b.WriteString("</tr>")
				}
				b.WriteString("</tbody></table>\n")
			}
		case strings.HasPrefix(trimmed, "### "):
			flushPara()
			closeList()
			b.WriteString("<h4>" + inline(trimmed[4:]) + "</h4>\n")
		case strings.HasPrefix(trimmed, "## "):
			flushPara()
			closeList()
			b.WriteString("<h3>" + inline(trimmed[3:]) + "</h3>\n")
		case strings.HasPrefix(trimmed, "# "):
			flushPara()
			closeList()
			b.WriteString("<h2>" + inline(trimmed[2:]) + "</h2>\n")
		case strings.HasPrefix(trimmed, "- ") || strings.HasPrefix(trimmed, "* "):
			flushPara()
			if list != "ul" {
				closeList()
				b.WriteString("<ul>\n")
				list = "ul"
			}
			b.WriteString("<li>" + inline(trimmed[2:]) + "</li>\n")
		case regexp.MustCompile(`^\d+\. `).MatchString(trimmed):
			flushPara()
			if list != "ol" {
				closeList()
				b.WriteString("<ol>\n")
				list = "ol"
			}
			b.WriteString("<li>" + inline(trimmed[strings.Index(trimmed, " ")+1:]) + "</li>\n")
		default:
			para = append(para, trimmed)
		}
	}
	flushPara()
	closeList()
	if inCode {
		b.WriteString("</code></pre>\n")
	}
	return template.HTML(b.String())
}
