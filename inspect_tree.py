import json
from reports import build_tree

if __name__ == '__main__':
    tree = build_tree('APP_v2', predecessor_root='APP_v1')
    print(json.dumps(tree, indent=2))
