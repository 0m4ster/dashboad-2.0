#!/usr/bin/env python3
"""
Script para corrigir conflitos de merge no arquivo api_kolm.py
"""

import re

def fix_merge_conflicts(file_path):
    """Corrige conflitos de merge no arquivo especificado"""
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Remove marcadores de conflito de merge
    # Remove <<<<<<< HEAD até =======
    content = re.sub(r'<<<<<<< HEAD\s*\n(.*?)\n=======\s*\n', r'\1\n', content, flags=re.DOTALL)
    
    # Remove >>>>>>> commit_hash
    content = re.sub(r'>>>>>>> [a-f0-9]+\s*\n', '', content)
    
    # Remove linhas vazias duplicadas
    content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
    
    # Salva o arquivo corrigido
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Conflitos de merge corrigidos em {file_path}")

if __name__ == "__main__":
    fix_merge_conflicts("api_kolm.py") 