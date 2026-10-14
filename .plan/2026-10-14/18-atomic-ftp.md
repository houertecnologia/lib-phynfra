# Implementação do Módulo FTP Atômico e Sincronização S3

## User Prompt
> Crie o arquivo phynfra/atomic/ftp.py contendo a classe Ftp para operações e sincronização de arquivos: utilize a biblioteca nativa ftplib (FTP e FTP_TLS) e tempfile; no construtor __init__(self, host, user=None, password=None, port=21, secure=False, timeout=60), inicialize e autentique a conexão armazenando-a em self._client; implemente o método download(self, remotePath, localPath=None) que baixa o arquivo remoto via retrbinary gravando no localPath informado (ou em arquivo temporário via tempfile.NamedTemporaryFile caso omitido) e retorna o caminho local absoluto; implemente o método sync(self, remotePath, bucket, keyPrefix='', s3=None) que lista os arquivos do diretório remoto via nlst/mlsd, verifica se os arquivos já existem no bucket S3 (utilizando instância de phynfra.aws.s3.S3) comparando tamanho e faz upload apenas dos arquivos novos ou modificados com S3.write, retornando lista dos arquivos sincronizados com sucesso; adicione método close(self) para encerrar a conexão, mantendo indentação com tabs e nomes em camelCase.

---

## Análise e Requisitos

1. **Imports e Estrutura de `phynfra/atomic/ftp.py`**:
   - `import os`
   - `import io`
   - `import tempfile`
   - `from ftplib import FTP, FTP_TLS`
   - `from phynfra.aws.s3 import S3`

2. **Classe `Ftp` e `__init__`**:
   - Assinatura: `def __init__ (self, host: str, user: str = None, password: str = None, port: int = 21, secure: bool = False, timeout: int = 60):`
   - Validação de `host`.
   - Inicialização com `FTP_TLS` caso `secure=True`, ou `FTP` caso `False`.
   - Conexão e autenticação com `login()`. Se `secure`, executar `prot_p()`.
   - Armazenar em `self._client`.

3. **Método `download`**:
   - Assinatura: `def download (self, remotePath: str, localPath: str = None) -> str:`
   - Validação de `remotePath`.
   - Download com `retrbinary`.
   - Gravação em `localPath` (se informado) ou arquivo temporário via `tempfile.NamedTemporaryFile(delete=False)`.
   - Retorno do caminho absoluto local.

4. **Método `sync`**:
   - Assinatura: `def sync (self, remotePath: str, bucket: str, keyPrefix: str = '', s3: S3 = None) -> list:`
   - Validação de `remotePath`, `bucket` e `s3` (instância de `S3`).
   - Listagem dos arquivos remotos via `mlsd` (com fallback para `nlst` e verificação de `size()`).
   - Escaneamento de arquivos existentes no bucket S3 com `s3.scan()`, mapeando chave e tamanho em bytes.
   - Comparação de tamanho para ignorar arquivos inalterados e upload de arquivos novos ou modificados via `s3.write()`.
   - Retorno de lista de arquivos sincronizados com sucesso.

5. **Método `close`**:
   - Assinatura: `def close (self) -> None:`
   - Encerramento seguro da conexão FTP (`quit` ou `close`).

6. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Criar o Arquivo `phynfra/atomic/ftp.py` [done]
- Implementar a classe `Ftp` com `__init__`, `download`, `sync` e `close`.

---

## Análise de Segurança
- **Comunicação Segura (FTPS)**: Suporte nativo a TLS com `FTP_TLS` e proteção do canal de dados via `prot_p()`.
- **Prevenção de Sobrecarga de Rede / Recursos**: Sincronização diferencial baseada em tamanho evita downloads e uploads redundantes no S3.
- **Fechamento e Limpeza**: Encerramento formal de conexões e sockets via `close()`.
