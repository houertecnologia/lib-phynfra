# Implementação do Método Slack.card

## User Prompt
> No arquivo phynfra/slack/__init__.py, adicione na classe Slack o método card(self, channel, title, status='info', details=None, buttonText=None, buttonUrl=None) para envio de mensagens estruturadas com Slack Block Kit: valide channel e title; defina um dicionário de status com ícones {'success': '🟢', 'error': '🔴', 'warning': '🟡', 'info': 'ℹ️'}; monte a lista blocks iniciando com um header contendo f"{statusIcon} {title}"; caso details seja fornecido como dict, adicione uma section com campos (fields) em markdown formatando f"*{k}:*\n{v}"; caso buttonText e buttonUrl sejam informados, adicione um bloco do tipo actions com elemento button contendo style ('primary' se success, 'danger' se error) redirecionando para a url informada; dispare a mensagem via self.__client.chat_postMessage(channel=channel, text=title, blocks=blocks) retornando a resposta da API e capturando SlackApiError com RuntimeError, mantendo indentação com tabs e nomes em camelCase.

---

## Análise e Requisitos

1. **Imports e Estrutura de `phynfra/slack/__init__.py`**:
   - `WebClient` e `SlackApiError` de `slack_sdk` (já importados).

2. **Assinatura e Parâmetros**:
   - `def card (self, channel: str, title: str, status: str = 'info', details: dict = None, buttonText: str = None, buttonUrl: str = None):`

3. **Validação de Entradas (Guard Clauses)**:
   - Validar se `channel` e `title` são strings não vazias.

4. **Construção dos Blocos (Slack Block Kit)**:
   - Mapear ícones: `statusIcons = {'success': '🟢', 'error': '🔴', 'warning': '🟡', 'info': 'ℹ️'}`.
   - Bloco `header` com texto `f'{statusIcon} {title}'`.
   - Se `details` for dict: bloco `section` com `fields` formatados como `f'*{k}:*\n{v}'`.
   - Se `buttonText` e `buttonUrl` forem informados: bloco `actions` contendo `button` com `style` (`'primary'` se `success`, `'danger'` se `error`) e `url`.

5. **Envio e Tratamento de Erros**:
   - Executar `self.__client.chat_postMessage(channel=channel, text=title, blocks=blocks)`.
   - Capturar `SlackApiError` levantando `RuntimeError`.
   - Retornar a resposta da API.

6. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Implementar o Método `Slack.card` em `phynfra/slack/__init__.py` [done]
- Adicionar o método `card` na classe `Slack`.

---

## Análise de Segurança
- **Proteção de Tokens e Credenciais**: `self.__token` e requisições HTTP continuam protegidos e encapsulados no cliente oficial `WebClient`.
- **Validação de URLs e Entradas**: Assegura sanitização básica de títulos e canais, prevenindo payloads vazios para a API do Slack.
