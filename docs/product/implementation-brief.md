# Implementation Brief — fechar US-106 e entregar US-107

**Estado em 26/09/2026:** o MVP e os incrementos locais têm evidência; a primeira CI pública falhou, a correção está no checkout local e ainda precisa de execução remota verde. O ensaio local de `ObservaDomainTargetDown` passou. Consulte o [relatório pós-MVP](post-mvp-report.md).

## Objetivo da próxima entrega

Fazer a CI pública validar o mesmo conjunto de checks já executado localmente e deixar o Observa reproduzível por um avaliador que comece pelo README. O resultado demonstrável é um commit com workflow verde, evidência operacional sanitizada e roteiro de diagnóstico seguido em checkout separado.

## Escopo e sequência

1. **US-106:** confirmar no log da [execução falha](https://github.com/mitsuoleo/observa/actions/runs/36258516353) a etapa e a mensagem exatas. O workflow tentou instalar Gitleaks com um caminho diferente daquele declarado pelo módulo da versão fixada. O patch local corrige esse caminho e desliga o cache Go, pois o repositório não tem `go.mod` ou `go.sum`. Repetir CI após envio aprovado; caso surja outra falha, diagnosticar a etapa e corrigir somente a causa, sem remover checks.
2. **US-103:** manter no relatório o baseline 30/30, o alerta Payment disparado e resolvido, o `demo` pós-recuperação e a carga 60/60 a 5 pedidos/s. Preservar a tentativa falha a 12 pedidos/s como limite observado. O script `alert-verify.ps1` permite repetir o ensaio com restauração automática.
3. **US-107:** o checkout separado do commit local `ff81956` passou em instalação, `up`, `status`, `contract`, `demo`, `recovery` e Grafana métrica → trace → logs → trace. O passo implícito de pausar o refresh foi documentado. Publicar só contagens, estados, horários e limites; artefatos brutos continuam em `.local/`.

## Contexto técnico comprovado

- O workflow em `.github/workflows/check.yml` roda em `ubuntu-latest`, instala Node 22, Python 3.12, Go e `kubectl`, instala Gitleaks e chama `scripts/check.ps1`.
- `check.ps1` cobre Node, Python, relays PostgreSQL, imagens, auditorias, segredos, manifests e regras. A CI não sobe minikube; a jornada real é verificada localmente por `mvp.ps1` e pelos runbooks.
- A stack local usa o perfil/namespace `observa-spike0`. KEDA mantém uma réplica mínima dos serviços; o ensaio de alerta pausa temporariamente Payment em zero e depois restaura o ScaledObject.
- O OrderFlow é referência somente leitura. O contrato Kafka e os três resultados de domínio do Observa já estão especificados no [PRD](prd.md) e no [contrato](../architecture/kafka-contract.md).

## Aceite e verificações

- A CI pública termina verde para o commit entregue, mantendo todas as fases do `check.ps1`; o link da execução entra no relatório.
- O check local completo, o diff final e a varredura de segredos são revisados antes de qualquer push.
- O ensaio do alerta mostra `firing`, serviço pronto e alerta resolvido; `demo` mostra `CONFIRMED`, `FAILED` e `CANCELLED` sem efeitos duplicados nos pedidos verificados.
- Um checkout separado encontra no README os pré-requisitos, comandos e links necessários; o percurso Grafana é inspecionado visualmente. Qualquer etapa não executada deve ser marcada como não verificada.
- O relatório distingue hipótese de 5 s, resultado local, teste sintético de regra, alerta real e falha da carga mais intensa.

## Riscos, parada e autoridade

- Não executar carga, alerta, chaos ou `down` simultaneamente no cluster. Se o ScaledObject não voltar ao estado inicial ou Payment não ficar pronto, parar novos ensaios e recuperar o ambiente antes de prosseguir.
- Se a CI falhar, registrar etapa, log e causa antes de editar; não enfraquecer testes, auditorias ou controles de segredo para obter verde.
- O executor pode ajustar detalhes internos reversíveis, documentação e testes compatíveis com as decisões existentes. Deve retornar ao usuário antes de alterar a arquitetura, quebrar contratos públicos, introduzir cloud ou dependência paga, reduzir a jornada demonstrável ou publicar mudanças remotas sem a aprovação exigida pelas instruções do projeto.
