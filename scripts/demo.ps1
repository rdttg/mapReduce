Write-Host "Subindo o cluster..."
docker compose up --build -d
Write-Host "Abra http://localhost:8080 e envie um trabalho com atraso de 10 segundos."
Write-Host "Falha worker: docker compose stop node1"
Write-Host "Retorno worker: docker compose start node1"
Write-Host "Falha líder: docker compose stop node3"
Write-Host "Retorno líder: docker compose start node3"
