import ast

class SecurityValidator(ast.NodeVisitor):
    def __init__(self):
        self.is_safe = True
        self.errors = []
        # Strictly block modules that interface with OS or Network
        self.forbidden_modules = {'os', 'sys', 'subprocess', 'requests', 'pathlib', 'builtins'}
        self.forbidden_functions = {'eval', 'exec', 'open', '__import__', 'getattr', 'setattr'}

    def visit_Import(self, node):
        for alias in node.names:
            if alias.name.split('.')[0] in self.forbidden_modules:
                self.is_safe = False
                self.errors.append(f"Forbidden module: {alias.name}")
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        if node.module and node.module.split('.')[0] in self.forbidden_modules:
            self.is_safe = False
            self.errors.append(f"Forbidden module: {node.module}")
        self.generic_visit(node)

    def visit_Call(self, node):
        # Catch direct calls like open()
        if isinstance(node.func, ast.Name) and node.func.id in self.forbidden_functions:
            self.is_safe = False
            self.errors.append(f"Forbidden function: {node.func.id}")
        
        # Catch attribute calls like pd.os.system()
        if isinstance(node.func, ast.Attribute):
            curr = node.func
            while isinstance(curr, ast.Attribute):
                curr = curr.value
            if isinstance(curr, ast.Name) and curr.id in self.forbidden_modules:
                self.is_safe = False
                self.errors.append(f"Forbidden module access: {curr.id}")
        self.generic_visit(node)

def audit_script(file_path: str) -> bool:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            code = f.read()
            if not code.strip(): return False
            tree = ast.parse(code)
        v = SecurityValidator()
        v.visit(tree)
        # Verify the AI included the required function entry point
        has_func = any(isinstance(n, ast.FunctionDef) and n.name == 'transform_supplier_data' for n in tree.body)
        if not has_func: return False
        return v.is_safe
    except:
        return False