# Inclusão de Dependências Ausentes no pyproject.toml

## User Prompt
> autorizado (para inclusão de python-docx, pillow e psycopg2-binary no pyproject.toml)

---

## Análise e Requisitos

1. **Dependências a Adicionar em `pyproject.toml`**:
   - `python-docx = "*"`: suporte a `phynfra/microsoft/word.py`.
   - `pillow = "*"`: suporte a `phynfra/microsoft/word.py`.
   - `psycopg2-binary = "*"`: driver PostgreSQL para `phynfra/database/postgresql.py`.

---

## Passos do Plano

### Step 1: Atualizar `pyproject.toml` [done]
- Incluir as dependências `python-docx = "*"`, `pillow = "*"` e `psycopg2-binary = "*"` em `[tool.poetry.dependencies]`.

---

## Análise de Segurança
- **Gerenciamento de Dependências (SCA)**: Garantia de integridade e resolução determinística das dependências necessárias em runtime para os novos módulos.
