# Implementação do Módulo Word para Templates DOCX Dinâmicos

## User Prompt
> Crie o arquivo phynfra/microsoft/word.py contendo a classe Word para inspeção e preenchimento dinâmico de relatórios baseados em templates docx: importe io, re, docx (Document, Pt, Inches, RGBColor), PIL (Image), pandas como pd e a classe S3 de phynfra.aws.s3; no construtor __init__(self, template, s3=None), aceite caminho local em string, objeto io.BytesIO ou URI do S3 (fazendo download dos bytes via S3), inicializando self._document = Document(buffer); implemente o método placeholders(self) que varre parágrafos e células de tabelas extraindo todas as tags no formato r'\{\$([a-zA-Z0-9_]+)\}' e retorna um dicionário categorizado {'inline': [...], 'table': [...], 'image': [...]} de acordo com os prefixos 'in_', 'tb_' e 'im_'; implemente o método render(self, context) que itera pelas tags substituindo 'in_' por texto formatado, 'im_' por imagem redimensionada (via add_picture a partir de caminho ou bytes) e 'tb_' por tabela estruturada construída a partir de DataFrame ou lista de dicts com cabeçalhos em negrito e preenchimento de células; implemente save(self, destination) gravando no disco local ou enviando para o S3 via S3.write, mantendo nomes em camelCase e indentação com tabs.

---

## Análise e Requisitos

1. **Imports e Dependências em `phynfra/microsoft/word.py`**:
   - `import os`
   - `import io`
   - `import re`
   - `from urllib.parse import urlparse`
   - `import docx`
   - `from docx import Document`
   - `from docx.shared import Pt, Inches, RGBColor`
   - `from PIL import Image`
   - `import pandas`
   - `from phynfra.aws.s3 import S3`

2. **Classe `Word` e `__init__`**:
   - Assinatura: `def __init__ (self, template, s3 = None):`
   - Tratar `template`: caminho local, `io.BytesIO` ou URI do S3 (`s3://` ou `s3a://` baixando via instância de `S3`).
   - Inicializar `self._document = Document(self._buffer)`.

3. **Método `placeholders`**:
   - Assinatura: `def placeholders (self) -> dict:`
   - Varrer parágrafos do documento e células de tabelas.
   - Extrair tags no formato `r'\{\$([a-zA-Z0-9_]+)\}'`.
   - Categorizar em `inline` (prefixo `in_`), `table` (prefixo `tb_`) e `image` (prefixo `im_`).
   - Retornar `{'inline': [...], 'table': [...], 'image': [...]}`.

4. **Método `render`**:
   - Assinatura: `def render (self, context: dict) -> 'Word':`
   - Substituir `in_` por texto correspondente.
   - Substituir `im_` por imagem (adicionando via `add_picture` a partir de caminho ou bytes com redimensionamento proporcional).
   - Substituir `tb_` por tabela estruturada construída a partir de `DataFrame` ou lista de dicts com cabeçalhos em negrito e preenchimento de células.
   - Retorno fluente de `self`.

5. **Método `save`**:
   - Assinatura: `def save (self, destination: str):`
   - Gravar em disco local (criando diretórios pais se necessário) ou enviar para o S3 via `S3.write`.

6. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Criar o Arquivo `phynfra/microsoft/word.py` [done]
- Implementar a classe `Word` com `__init__`, `placeholders`, `render` e `save`.

---

## Análise de Segurança
- **Proteção de Buffer em Memória**: Carregamento e manipulação de documentos em buffers `BytesIO`, evitando arquivos temporários desprotegidos em disco.
- **Validação de Destinos e Imagens**: Validação de dimensões e tipos de arquivos antes de inserção no XML do documento para prevenir corrupção estrutural do pacote OpenXML (docx).
