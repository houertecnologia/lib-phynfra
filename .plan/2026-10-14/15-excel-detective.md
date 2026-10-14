# Implementação do Módulo Excel e Método detective

## User Prompt
> Crie o arquivo phynfra/microsoft/excel.py contendo a classe Excel para manipulação e detecção inteligente de cabeçalhos em planilhas: importe io, re, unicodedata, pandas como pd, openpyxl, a função sanitize de phynfra.atomic.utils, S3 de phynfra.aws.s3 e DataFramer e Spark de phynfra.apache.spark; no __init__(self, source, spark=None, s3=None), trate source como bytes, io.BytesIO, caminho local em string ou URI do S3 (s3:// ou s3a:// fazendo download dos bytes via instância de S3), armazenando em self._buffer como BytesIO e instanciando self._excelFile = pd.ExcelFile(self._buffer); implemente o método detective(self, expectedColumns, minRatio=0.6, spark=None) que itera pelas abas de self._excelFile.sheet_names e varre até as primeiras 20 linhas normalizando células com sanitize, localiza a aba e a linha que correspondam a pelo menos minRatio das colunas esperadas, lê a aba completa via pd.read_excel(self._buffer, sheet_name=sheetName, header=headerRow), converte o Pandas DataFrame resultante para Spark DataFrame utilizando a sessão Spark ativa (de self._spark ou do argumento), e retorna a instância de DataFramer já carregada e pronta para encadeamento, seguindo rigorosamente a indentação com tabs e nomes em camelCase.
> User hint: usar `import pandas` diretamente sem alias `pd`.

---

## Análise e Requisitos

1. **Imports e Estrutura de `phynfra/microsoft/excel.py`**:
   - `import io`
   - `import re`
   - `import unicodedata`
   - `import pandas` (sem alias, conforme instrução do usuário)
   - `import openpyxl`
   - `from urllib.parse import urlparse`
   - `from phynfra.atomic.utils import sanitize`
   - `from phynfra.aws.s3 import S3`
   - `from phynfra.apache.spark import DataFramer, Spark`

2. **Classe `Excel` e `__init__`**:
   - Assinatura: `def __init__ (self, source, spark = None, s3 = None):`
   - Suporte a `source`:
     - `io.BytesIO`: armazena cópia em `self._buffer`.
     - `bytes` / `bytearray`: armazena em `self._buffer = io.BytesIO(source)`.
     - `str`:
       - `s3://` ou `s3a://`: efetua download dos bytes via instância de `S3` (validando `s3`), gravando em `self._buffer`.
       - Caminho local: lê os bytes em disco e grava em `self._buffer`.
   - Inicializar `self._excelFile = pandas.ExcelFile(self._buffer, engine = 'openpyxl')`.

3. **Método `detective`**:
   - Assinatura: `def detective (self, expectedColumns, minRatio: float = 0.6, spark = None) -> DataFramer:`
   - Normalizar `expectedColumns` com `sanitize`.
   - Varrer as abas em `self._excelFile.sheet_names` e as primeiras 20 linhas de cada aba.
   - Normalizar células de cada linha com `sanitize`.
   - Calcular similaridade / correspondência (`ratio = len(intersection) / len(expected)`).
   - Identificar a aba e a linha do cabeçalho que alcancem `minRatio` (selecionando a melhor correspondência).
   - Ler a aba inteira via `pandas.read_excel(self._buffer, sheet_name=bestSheet, header=bestHeaderRow)`.
   - Resolver sessão Spark ativa (`spark` ou `self._spark`) e converter o Pandas DataFrame para Spark DataFrame via `DataFramer.convertFrom`.
   - Retornar a instância de `DataFramer` carregada.

4. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Criar o Arquivo `phynfra/microsoft/excel.py` [done]
- Implementar a classe `Excel` com `__init__` multiformato e método `detective`.

---

## Análise de Segurança
- **Proteção de Acesso a Arquivos e S3**: Validação explícita de URIs e dependência de autenticação válida para download do S3.
- **Isolamento de Memória (BytesIO)**: O processamento de planilhas é mantido em buffer de memória, evitando arquivos residuais desprotegidos em disco temporário.
- **Validação de Entrada e Limites de Leitura**: O escopo de varredura é limitado a 20 linhas por aba (`nrows=20`), mitigando ataques de DoS por planilhas excessivamente grandes.
