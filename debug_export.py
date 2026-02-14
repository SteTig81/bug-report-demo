from reports import build_tree, export_html

t = build_tree('APP_v2', predecessor_root='APP_v1')
export_html(t, 'reports/debug_test.html', 'Bug-Report')
print('wrote debug_test.html')
