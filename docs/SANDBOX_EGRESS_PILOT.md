# Selective Squid → host Xray: контракт Pulsy и пилот

## Граница поставки

Платформа предоставляет `SandboxEgressUpstream` и общий Squid; routing контракт
доступен начиная с 0.8.0. Новый `DockerEgressHostRelay` доступен начиная с 0.9.0 и требует отдельного
обновления package: релиз 0.8.1 его ещё не содержит.
Pulsy должен пересобрать приложение и передать новую настройку при bootstrap.
Обновление Pulsy и изменения host выполняются отдельно от релиза платформы.
Команды ниже предназначены для оператора после отдельного согласования.

Путь: песочница/credential broker → общий Squid → назначенный группе HTTP
upstream. Группа OpenAI в пилоте использует host bridge listener → существующий
`127.0.0.1:10809` Xray. Другим группам можно назначить другие HTTP proxy URL. Остальные hostname
Squid обслуживает напрямую. Один общий relay на host сохраняет текущий Xray
inbound и его проверенный маршрут; отдельные proxy-контейнеры не нужны.
Политика host-wide: группы и их назначения одинаковы для всех песочниц этого host.
`small=512m`, `max_active_sandboxes=3` в Pulsy сохраняются.

## Контракт для отдельной задачи Pulsy

Настройка routes текущего Pulsy pilot (по принятому контексту задачи):

```dotenv
PULSY_CODEX_EGRESS_UPSTREAM_ROUTES=[{"proxy_url":"http://host.docker.internal:10810","destination_hosts":["api.openai.com","auth.openai.com","chatgpt.com"]}]
```

Pulsy разбирает JSON-массив групп и конструирует типизированный tuple:

```python
from soveren_agent_platform.sandbox import DockerEgressHostRelay, SandboxEgressUpstream

egress_upstreams = tuple(
    SandboxEgressUpstream(
        proxy_url=group.proxy_url,
        destination_hosts=tuple(group.destination_hosts),
    )
    for group in settings.codex_egress_upstream_routes
)
# В существующем configure_sandboxed_codex:
# resources="small", max_active_sandboxes=3, egress_upstreams=egress_upstreams
# Дополнительная Docker/host настройка после обновления platform package:
# egress_host_relays=(DockerEgressHostRelay(
#     proxy_url="http://host.docker.internal:10810",
# ),)
```

Настройка отсутствует/пустая либо `[]` — `egress_upstreams=()` и прямой трафик.
Каждой группе обязательны `proxy_url` и непустой `destination_hosts`; невалидная
группа или повтор hostname после нормализации — ошибка bootstrap. Повторы
запрещены и внутри группы, и между группами, даже с одинаковым proxy URL.
Разные hostname с одним proxy URL объединяются в один Squid peer. Максимум —
16 входных групп и 256 hostname суммарно. Никакого default на OpenAI в платформе
нет. Пример выше выбирает только три конкретных hostname: он не
обещает полноту списка для всех режимов Codex/login. Дополнительные фактические
endpoint-hostnames добавляются после проверки сетевых запросов. DeepSeek и
другие не перечисленные провайдеры остаются прямыми. Пути, имена моделей и
поддомены автоматически не сопоставляются.

`SOVEREN_EGRESS_UPSTREAM_ROUTES` с таким же JSON-массивом групп принадлежит
самому Squid-контейнеру. Manager передаёт его из типизированной политики;
для ручного Compose необходимо
задать точно такую же политику. Одних `HTTPS_PROXY` в Pulsy недостаточно.

## На host: подготовка доступа

Выполнять в одной bash-сессии под административным пользователем Pulsy host
с `sudo` и Docker, не под ограниченным `codex_agent`. Используется обычный
rootful Docker с iptables, как требуется существующим sandbox runtime.
Xray остаётся на loopback; новый socket слушает только IPv4 Docker bridge.
Host firewall допускает только текущие IP **и MAC** общего Squid на его public
uplink. Любые другие источники, включая соседние контейнеры, отбрасываются.

### 1. Проверить исходное состояние

```bash
set -euo pipefail
sudo ss -ltnp 'sport = :10809'
curl --noproxy '' --proxy http://127.0.0.1:10809 --max-time 20 \
  --silent --show-error --output /dev/null --write-out '%{http_code}\n' \
  https://api.openai.com/v1/models
egress_host_gateway=$(sudo docker network inspect bridge \
  --format '{{(index .IPAM.Config 0).Gateway}}')
python3 -c 'import ipaddress,sys; a=ipaddress.ip_address(sys.argv[1]); assert a.version == 4 and a.is_private' \
  "$egress_host_gateway"
ip -4 address show
test -x /usr/lib/systemd/systemd-socket-proxyd
sudo docker inspect soveren-sandbox-egress \
  --format '{{with index .NetworkSettings.Networks "soveren-sandbox-public-egress"}}{{.IPAddress}} {{.MacAddress}}{{end}}'
```

Ожидается loopback listener Xray и ответ `401` без provider auth — это проверка
доступности, не рабочего логина/модели. Проверить, что полученный bridge gateway
действительно назначен host. При отличающемся пути systemd binary, bridge или
firewall backend остановиться и адаптировать операцию к фактам host.

### 2. Установить firewall helper и socket relay

Для обновления текущего pilot сначала штатно остановить Pulsy и активные turns.
В согласованное окно остановить relay socket и service перед изменением helper:

```bash
if sudo systemctl cat soveren-xray-bridge.socket >/dev/null 2>&1; then
    sudo systemctl stop soveren-xray-bridge.socket soveren-xray-bridge.service
fi
```

`Requires=` распространяет restart firewall unit на relay, поэтому обновление
не выполняется во время активных turns. Loopback Xray остаётся запущенным.

Для нового managed контракта boot helper устанавливает постоянный DROP в
собственной legacy цепочке. Runtime размещает узкий Squid-only guard над ней
при acquire. Helper не очищает host firewall и не трогает conversation
`DOCKER-USER`/`INPUT` drop rules или `SVR-RELAY-*` цепочки. Обновление legacy
цепочки выполняется одним `iptables-restore` commit; оно не зависит от наличия
Squid при boot. Установить эту версию helper только вместе с bootstrap
`egress_host_relays`: без настройки runtime selected остаётся закрытым.

```bash
sudo tee /usr/local/sbin/soveren-xray-firewall >/dev/null <<'SH'
#!/bin/bash
set -euo pipefail
bridge_gateway="$1"
python3 - "$bridge_gateway" <<'PY'
import ipaddress, sys
value = ipaddress.ip_address(sys.argv[1])
assert value.version == 4 and value.is_private and not value.is_loopback
PY
iptables-restore --wait 5 --noflush <<RULES
*filter
:SOVEREN-XRAY - [0:0]
-F SOVEREN-XRAY
-A SOVEREN-XRAY -j DROP
COMMIT
RULES
if ! iptables -w 5 -C INPUT -d "${bridge_gateway}/32" -p tcp --dport 10810 -j SOVEREN-XRAY; then
    iptables -w 5 -I INPUT 1 -d "${bridge_gateway}/32" -p tcp --dport 10810 -j SOVEREN-XRAY
fi
SH
sudo chmod 0755 /usr/local/sbin/soveren-xray-firewall

sudo tee /etc/systemd/system/soveren-xray-firewall.service >/dev/null <<UNIT
[Unit]
Description=Allow only shared Squid to the Xray bridge relay
Requires=docker.service
After=docker.service
Before=soveren-xray-bridge.socket

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/local/sbin/soveren-xray-firewall ${egress_host_gateway}
UNIT

sudo tee /etc/systemd/system/soveren-xray-bridge.socket >/dev/null <<UNIT
[Unit]
Description=Private Docker bridge socket for shared Squid
Requires=soveren-xray-firewall.service
After=soveren-xray-firewall.service

[Socket]
ListenStream=${egress_host_gateway}:10810
Accept=no

[Install]
WantedBy=sockets.target
UNIT

sudo tee /etc/systemd/system/soveren-xray-bridge.service >/dev/null <<'UNIT'
[Unit]
Description=Forward bridge proxy socket to existing loopback Xray
Requires=soveren-xray-bridge.socket
After=soveren-xray-bridge.socket

[Service]
ExecStart=/usr/lib/systemd/systemd-socket-proxyd 127.0.0.1:10809
DynamicUser=yes
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
UNIT

sudo systemctl daemon-reload
sudo systemctl restart soveren-xray-firewall.service
sudo iptables -S SOVEREN-XRAY
# Проверить: в legacy цепочке только DROP, ни одной IP/MAC ACCEPT-записи.
sudo systemctl enable --now soveren-xray-bridge.socket
sudo systemctl is-active soveren-xray-firewall.service soveren-xray-bridge.socket
sudo iptables -S SOVEREN-XRAY
sudo ss -ltnp 'sport = :10810'
```

Не публиковать port через Docker и не менять Xray listen на `0.0.0.0`.
Установленный firewall helper заново выполняется при boot через dependency
socket unit. Listener может подняться до Squid, но INPUT остаётся DROP до
managed acquisition. При изменении bridge gateway необходимо обновить оба
unit-файла перед продолжением. Этот helper не обновляет runtime allowlist и
не создаёт background watcher.

### 3. После согласованных release и отдельного обновления Pulsy

Остановить приём новой работы и активные sandbox turns штатным shutdown Pulsy.
Обновить package/images и настройки Pulsy. При первом acquire manager заменит
общий Squid, сохранив sandbox workspace и сетевые drop rules. Перед первым
sandbox turn manager синхронно обновляет собственную `SVR-RELAY-*` INPUT
цепочку по актуальным IP/MAC Squid, затем проверяет TCP доступность только
назначенного host relay. В Pulsy bootstrap необходимо передать
`egress_host_relays`, как показано выше. Без этой новой настройки сохраняется
прежний ручной контракт, включая restart firewall unit после пересоздания
Squid. Для настроенного managed relay ручной restart на каждой замене больше
не требуется. Проверить после acquisition:

```bash
sudo iptables -S INPUT
sudo docker exec soveren-sandbox-egress getent ahostsv4 host.docker.internal
sudo docker exec soveren-sandbox-egress squid -k parse -f /run/soveren-squid.conf
sudo docker exec soveren-sandbox-egress cat /run/soveren-squid.conf
```

Адрес `host.docker.internal` должен совпадать с `egress_host_gateway`.
Если Docker переопределяет `host-gateway`, исправить явную конфигурацию
listener/upstream, не расширять firewall allowlist. Manager размещает узкий
endpoint guard первым в INPUT, выше прежней `SOVEREN-XRAY` цепочки; внутри
разрешены только новый Squid IP/MAC и адрес/порт relay, затем DROP. Старая
allow-запись и INPUT hook заменяются одной firewall transaction без временного
DROP для работающего поколения; перед штатной заменой Squid allow отзывается.
Отмена acquisition дожидается завершения именованного mutation helper под
egress lock. При timeout/потерянном ответе helper останавливается через Docker;
если cleanup не подтверждён, setup завершается ошибкой. Retry сначала проверяет
и завершает предыдущий owned helper, а затем применяет новые правила.
Bootstrap helper из раздела 2 остаётся boot guard: до acquisition listener
защищён операторской default-deny политикой. Операторские сервисы не должны
переставлять managed INPUT guard или менять `SVR-RELAY-*` цепочки. Один
control plane владеет shared egress этого host.

При переходе со старого pilot helper оператор заменяет его версией из раздела 2
с постоянным DROP вместо динамической allow. Managed цепочка над ней разрешает
актуальный Squid, а после reboot старая identity не остаётся разрешённой.
Это отдельное согласованное изменение host.

Событие `docker_egress_host_relay_probe` сообщает `proxy_url` и `available`.
Неуспешный двухсекундный TCP probe не блокирует acquisition или direct/другие
группы; selected сохраняет Squid fail-closed. Это не проверка Xray маршрута,
TLS, OAuth или provider auth. Ошибка установки firewall, неверная identity или
relay IP, не назначенный host, останавливает setup как нарушение изоляции.
Не обходить её разрешением Docker subnet. При удалении managed relay из
настроек его цепочка остаётся DROP, listener отключается отдельно оператором.

### 4. Проверить маршруты из существующей песочницы

Выбрать фактическую песочницу пилота по `docker ps`, затем использовать её ID
вместо `SANDBOX_ID`. Никакие токены для проверки не нужны.

```bash
sudo docker ps --filter label=soveren.runtime=docker
sudo docker exec SANDBOX_ID curl --noproxy '' \
  --proxy http://soveren-sandbox-egress:3128 --max-time 20 \
  --silent --show-error --output /dev/null --write-out '%{http_code}\n' \
  https://api.openai.com/v1/models
sudo docker exec SANDBOX_ID curl --noproxy '' \
  --proxy http://soveren-sandbox-egress:3128 --max-time 20 \
  --silent --show-error --output /dev/null --write-out '%{http_code}\n' \
  https://api.deepseek.com/
sudo docker exec soveren-sandbox-egress tail -n 30 /var/log/squid/access.log
```

Для OpenAI ожидается `401`, а Squid hierarchy — parent (например
`FIRSTUP_PARENT`), без `HIER_DIRECT`. Для DeepSeek HTTP-код не служит доказательством
маршрута: проверить `HIER_DIRECT` и отсутствие этой цели в parent-маршрутах.
Отдельно проверить используемый Pulsy ChatGPT endpoint и настоящий Codex turn
через его выбранный credential mode.

В согласованное окно отказа остановить **только relay**, повторить selected и
direct запросы и затем обязательно вернуть socket:

```bash
sudo systemctl stop soveren-xray-bridge.socket soveren-xray-bridge.service
# Повторить оба curl выше: selected -> proxy error, direct -> прежний маршрут.
sudo systemctl start soveren-xray-bridge.socket
```

Selected должен получить proxy error (обычно 503), без `HIER_DIRECT` и без
обращения напрямую к provider или к proxy другой группы. В том же окне direct
и другие настроенные группы должны продолжать работать. Для каждой дополнительной
группы проверить назначенный proxy по Squid hierarchy и повторить окно отказа
её upstream, подтвердив отсутствие перехода к другим parent.
Повторить selected после восстановления (учесть короткий Squid parent retry).
Из песочницы запрос через Squid к самому host listener, loopback и metadata
должен дать 403; прямое подключение к listener без Squid должно быть заблокировано
существующей conversation INPUT-политикой. Соседний контейнер на public uplink
также не должен подключаться к relay из-за отдельного IP/MAC firewall guard.

## Что проверено в репозитории

`bash scripts/smoke_egress.sh` собирает настоящий pinned Squid и проверяет
generated config его parser, selected/direct HTTP и CONNECT с двумя разными
parent proxy, отказ и недоступность/recovery каждой группы,
private/metadata/unknown-DNS deny и отсутствие как прямых обращений, так и
перехода к чужому proxy при отказе. Путь начинается с публичного application bootstrap,
проходит manager Docker launch и image entrypoint. CONNECT-проверка использует
контрольные байты внутри tunnel, отдельно от TLS/auth.
Дополнительный oracle проходит настоящий `acquire` с Linux INPUT firewall и
host HTTP parent: создание и policy replacement Squid, repair stale IP/MAC,
selected HTTP/CONNECT из настоящего sandbox, отказ/recovery relay,
работоспособность direct/другой группы и запрет соседнему контейнеру.
Задержанный реальный helper проверяет повторную отмену соседнего acquire,
lost CLI response с отказом cleanup, retry, timeout, rollback ошибочной restore
transaction и отказ удаления foreign контейнера с занятым helper name.
Он требует Docker daemon без других managed workloads и удаляет собственные
контейнеры, сети и firewall rules после проверки. Это не live-проверка Xray или
Pulsy host: systemd units, boot и настоящий Codex turn требуют отдельной
операционной проверки в пилоте.

## Откат после managed cutover

Откатываются согласованно package, bootstrap и host policy. Одного downgrade
package недостаточно: managed INPUT guards остаются на host.

1. Штатно остановить Pulsy/turns и relay socket/service. Подтвердить, что
   именованные `soveren-svr-relay-*-apply` helpers завершились; чужие контейнеры
   с такими именами не удалять. При неизвестном результате оставаться закрытым.
2. Вернуть сохранённые прежние package/bootstrap и operator unit/helper files.
   До разрешения работы подготовить Squid с точными image/routes откатываемой
   конфигурации, чтобы следующий acquire не заменил его после обновления legacy
   allow. Проверить конфигурацию, health и фактические IP/MAC. Если подготовка
   не подтверждена, работу не возобновлять.
3. Выполнить `daemon-reload` и явный restart прежнего firewall helper, который
   устанавливает narrow legacy allow по этой фактической Squid identity.
4. Удалить только endpoint jumps к managed `SVR-RELAY-*` и их owned chains для
   откатываемых relay URL. Не очищать INPUT/DOCKER-USER, conversation rules или
   цепочки других сервисов; не разрешать Docker subnet.
5. Вернуть relay socket, проверить selected/direct маршруты, затем возобновить
   Pulsy. При последующем пересоздании Squid старый релиз снова требует ручного
   обновления legacy allow, как до managed cutover.

Принятая спецификация и критерии отказоустойчивости:
[SANDBOX_EGRESS_RECONCILIATION_SPEC.md](SANDBOX_EGRESS_RECONCILIATION_SPEC.md).

Основные механизмы: [Squid never_direct](https://www.squid-cache.org/Doc/config/never_direct/),
[cache_peer_access](https://www.squid-cache.org/Doc/config/cache_peer_access/),
[Docker host-gateway](https://docs.docker.com/reference/cli/docker/container/run/#add-entries-to-container-hosts-file---add-host),
[systemd socket-proxyd](https://github.com/systemd/systemd/blob/main/man/systemd-socket-proxyd.xml).
