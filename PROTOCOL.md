# Protocolo REST/JSON
Todos os nós expõem porta 8000 na rede interna. O painel expõe 8080. Comunicação entre nós: POST /message, Content-Type application/json.

```json
{"message_id":"uuid","message_type":"HEARTBEAT","sender_id":1,"receiver_id":2,"lamport_timestamp":12,"leader_epoch":[1,3],"payload":{}}
```
Resposta: `{"clock":13,"payload":{"ok":true}}`. Cada envio e recebimento atualiza Lamport; época é o par (contador, ID do líder). Versão do snapshot é (contador, ID do líder, revisão).

| Tipo | Payload | Resposta / finalidade |
|---|---|---|
| REGISTER | status do emissor | status local; registra presença |
| HEARTBEAT | status do emissor | status local; atualiza presença |
| ELECTION | vazio | ok e tipo OK se ID receptor superior |
| COORDINATOR | leader | ok; anuncia época e líder |
| STATE | vazio | state; consulta snapshot para recuperação |
| REPLICATE | snapshot com version e jobs | ok; persiste versão não antiga |
| ASSIGN | tarefa com id, job, fragment, delay, attempt, worker | ok; inicia MAP |
| RESULT | job, id, attempt, worker, result | ok; aceita somente tentativa vigente |

Status contém id, leader, ready, epoch, clock, busy, version. GET /health verifica processo; GET /status retorna status; GET /state retorna snapshot; GET /events retorna eventos locais. POST /jobs recebe text, fragments e delay, retorna id (ou erro 422/503). Painel: GET /api/status agrega dados e POST /api/jobs encaminha ao líder pronto. Endpoints de leitura do painel não são mensagens de coordenação. Timeout HTTP entre nós: 0,8 segundo; heartbeat padrão 2 s; detecção padrão 6 s. Mensagens inválidas recebem 400/422. Não há TLS ou autenticação.
