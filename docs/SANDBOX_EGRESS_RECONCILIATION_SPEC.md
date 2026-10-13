# Безопасное согласование доступа Squid к host relay

## Принятый контекст

База реализации: свежая `origin/main`,
`810105c187de910f09286ea51d63c865862207a6`; ancestry проверена.
Первоначальная реализация сохранена отдельно в
`019dd6abd7bab5289726c24fe316450018affd0e`.
Эта спецификация фиксирует принятый после ревью план исправления.

Владелец механизма — Docker sandbox adapter. Source of truth — фактические
Docker ID, IPv4/MAC public uplink Squid, host mapping внутри Squid и текущие
правила Linux INPUT. Приложение задаёт parent URL и exact destination hosts.
Host listener и его постоянный boot-time DROP принадлежат оператору.

## Ожидаемый результат и границы

Перед первым sandbox turn после создания/замены Squid manager синхронно
согласует доступ к настроенному host relay и проверяет TCP доступность только
этого parent из Squid. Недоступный upstream остаётся ошибкой назначенной ему
группы: Squid не переходит на direct или чужой parent. Direct и другие группы
продолжают работать. TCP probe не доказывает TLS, provider auth или Xray route.

Сохраняются `SandboxEgressUpstream`, отдельный opt-in `DockerEgressHostRelay`,
IPv4-only isolation, capacity, workspace lifecycle и запрет ротации Squid при
работающих managed sandboxes. Нет Docker-subnet allow, provider-specific
логики, фонового watcher, новых публичных портов или общего изменения
`SubprocessDockerCommandRunner`. Release, push и изменения live host вне scope.

## Firewall и повторный acquire

1. Проверить фактическую Squid identity, разрешить endpoint внутри Squid и
   убедиться, что адрес назначен Linux Docker host, до изменения allowlist.
2. Завершить предыдущий owned helper и только затем прочитать INPUT без ссылки
   на ещё не существующую relay chain. Отсутствие
   цепочки при первом запуске — нормальное состояние, а не ошибка firewall.
3. Одним `iptables-restore --noflush` применить создание/замену собственной
   цепочки и перенос endpoint-specific INPUT hook наверх. В цепочке только
   актуальные source `/32` и MAC, destination `/32`, TCP port, затем DROP.
   Повторные exact hooks свести к одному. Чужие цепочки и INPUT policy сохранить.
4. На обычном acquire не устанавливать промежуточный DROP для исправного
   поколения. Ошибка транзакции сохраняет прежние правила целиком. Не
   восстанавливать прежнюю allowlist из `except`: она может быть уже устаревшей.
5. Cleanup неудачного разговора не откатывает общую relay policy другого
   работающего разговора. Не использовать cache «однажды готово» вместо
   фактической Docker/kernel identity.

## Отмена, таймаут и неоднозначный результат

Изменение firewall выполняет именованный временный helper с платформенными
ownership labels, host network, NET_ADMIN, `--init`, `--rm` и stdin payload.
Имя детерминировано relay endpoint, чтобы после crash/restart можно было
найти предыдущий helper. Чужой контейнер с таким именем не удаляется.

Только критическая mutation защищена от caller cancellation. Пока helper не
завершился или его остановка не подтверждена Docker, `_egress_lock` не
освобождается для следующего writer. Повторная отмена тоже дожидается этого
результата, затем передаётся вызывающему коду. Никакой mutation task не
остаётся работать фоном после возврата из acquire.

Ожидание apply и inspect/remove предыдущего helper ограничено отдельными
15-секундными budgets; `iptables-restore` использует `--wait 5`. При timeout,
ошибке CLI или потерянном ответе остановить принадлежащий платформе helper
через Docker и подтвердить удаление. Если результат cleanup неизвестен,
setup завершается ошибкой, а следующая попытка сначала удаляет/проверяет
предыдущий helper и лишь затем пишет правила. Уникальное Docker name не
позволяет запустить параллельный writer с той же идентичностью.

Ошибка identity, firewall или неподтверждённое завершение helper — ошибка
изоляции/setup, не обычная недоступность parent. Не превращать её в успешный
acquire. Для working conversations сохраняется последняя атомарно применённая
политика; отмена соседнего setup не должна оставлять намеренный временный DROP.

## Создание, замена, удаление и recovery

При замене сначала убедиться в отсутствии работающих managed sandboxes,
согласовать/завершить предыдущие helper операции и атомарно отозвать старые
relay allow. Только затем удалить старый Squid. Новое поколение допускается
к первому turn после проверки конфигурации, health и установки новой policy.
Если replace/health/setup прерван, повторная попытка идемпотентно продолжает
этот существующий Docker lifecycle. Прежний source не становится fallback.

При удалении relay setting его owned chain остаётся DROP; listener выключается
оператором отдельно. Retired helper не может запоздало восстановить старую allow.
После control-plane restart stale helper обнаруживается по имени и labels.

## Переход и откат на host

В согласованное окно остановить Pulsy/активные turns, relay socket и service.
Установить новый package/bootstrap и boot helper с постоянным DROP. После
`daemon-reload` явно перезапустить firewall oneshot и проверить отсутствие
legacy allow. Затем включить socket и запустить Pulsy; первый acquire открывает
узкий managed guard. `enable --now` не переисполняет активный oneshot.
`Requires=` может распространять restart на socket/service, поэтому операцию
не выполнять во время активных turns. Loopback service за relay не меняется.

Откат включает package, bootstrap и host policy вместе: остановить workload и
relay, подтвердить отсутствие выполняющихся managed helpers, подготовить
Squid с точными image/routes откатываемой конфигурации, восстановить legacy
helper и его актуальную Squid IP/MAC allow, удалить только managed
endpoint jumps/chains, затем восстановить relay/workload. Не очищать общий
firewall, conversation rules и не разрешать Docker subnet. При неизвестной
identity оставаться закрытым и восстановить её до запуска.

## Риски и критерии проверки

Применимые строки implementation-risk-matrix: access/source of truth;
atomicity/concurrency; lifecycle/recovery; failure/uncertain; contracts/rollout;
boundedness; production path и scope fidelity.

Обязательные oracles:

- Настоящий `AgentPlatformApp` bootstrap → `acquire` → создание Squid → INPUT:
  первая цепочка создаётся без предварительного открытия listener.
- Работающий sandbox A сохраняет relay при отмене setup B до apply, после
  apply и при повторной отмене во время helper. INPUT hook не дублируется.
- Задержанный реальный helper заканчивается до следующего writer; его поздний
  результат не перезаписывает более новую policy.
- Timeout/ошибка apply, lost CLI reply и отказ cleanup не дают false success;
  следующая попытка убирает оставшийся owned helper. Foreign name не удаляется.
- Невалидная restore transaction сохраняет прежние kernel rules и INPUT policy.
- Настоящая ротация с изменением IP/MAC, stale policy repair, parent refusal,
  unavailable/recovery, direct/вторая группа и retired relay проходят прежний
  real Squid oracle. Соседний контейнер и sandbox direct-to-relay запрещены.
- Нет разрешения старому helper удалить или перезаписать новое поколение.
- Host-инструкция содержит явный apply нового oneshot и согласованный rollback.

Финальный gate: `uv run ruff check src tests`, `uv run mypy`, `uv run pytest`
с включённым `SOVEREN_TEST_EGRESS_IMAGE` для настоящего Docker oracle.
Live Pulsy/systemd/provider acceptance отмечается отдельно от локального gate.
