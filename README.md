# Distributed Task Manager
Mini MapReduce acadêmico em Python/FastAPI. Três nós idênticos, cada qual com persistência JSON própria, e um painel independente.

## Executar no Windows
Instale e abra Docker Desktop (contêineres Linux). Extraia o ZIP, abra PowerShell na pasta que contém `compose.yaml` e execute:

```powershell
docker compose up --build -d
docker compose ps
```
Abra http://localhost:8080. Aguarde a eleição do NODE 3. O painel consulta os servidores reais e não simula falhas.

## Demonstração
1. Carregue o texto de exemplo, escolha 6 fragmentos e atraso 0; processe. Veja os resultados MAP e REDUCE.
2. Envie outro trabalho com atraso de 10 segundos. Quando NODE 1 estiver processando, execute `docker compose stop node1`. Após o timeout, observe a reatribuição e o resultado.
3. Execute `docker compose start node1` e aguarde a sincronização.
4. Envie um novo trabalho com atraso de 10 segundos e execute `docker compose stop node3` enquanto estiver em execução. NODE 2 deverá assumir e recuperar as tarefas.
5. Execute `docker compose start node3`; o maior ID retorna à liderança após sincronização.
6. Para acompanhar: `docker compose logs -f node1 node2 node3`.
7. Encerrar: `docker compose down`. Para apagar também os dados: `docker compose down -v` (destrutivo).

## Arquitetura e limites
DNS do Compose fornece endereços via PEERS; mensagens REGISTER e HEARTBEAT atualizam a presença. Bully usa ELECTION, resposta OK e COORDINATOR. Lamport ordena causalmente eventos, não mede tempo real. O líder divide palavras, atribui um fragmento por worker, recebe resultados e consolida por tarefa. Se não houver worker disponível, o líder pode executar MAP localmente. A tentativa UUID impede resultados duplicados e antigos. Cada nó persiste snapshots JSON por substituição atômica; uma submissão só é confirmada quando ao menos uma réplica responde. Recuperação escolhe a maior versão conhecida e reprocessa tarefas RUNNING. Um resultado pode ser executado mais de uma vez, mas não é somado duas vezes na mesma história de estado.

Modelo de falhas: parada/reinício de um nó por vez, rede íntegra entre os sobreviventes. Bully e snapshots não são consenso; partições podem produzir split-brain e perda de atualizações concorrentes. Não há garantia de consistência forte, execução exatamente uma vez ou durabilidade para falhas simultâneas. Um POST que retorna erro após replicação parcial pode ainda ter criado um trabalho; confira o painel antes de reenviar. O sistema não tem autenticação; use apenas localmente.

## Testes
Com Python e dependências instaladas: `python -m pip install -r requirements.txt pytest`, `python -m pytest -q`, `python tests/integration.py`. O teste de integração usa três processos reais HTTP em portas locais e mata/reinicia processos, sem depender do Docker. Valide também a execução em contêineres com os passos acima.
