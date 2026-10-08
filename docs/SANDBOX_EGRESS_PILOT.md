# Selective Squid → host Xray: контракт Pulsy и пилот

## Граница поставки

Платформа предоставляет `SandboxEgressUpstream` и общий Squid. Pulsy должен
отдельно обновить dependency на будущую согласованную версию платформы,
пересобрать приложение и передать политику при bootstrap. Опубликованная 0.7.1
этого контракта не поддерживает. Merge, release и изменения host не выполняются
этим PR. Команды ниже предназначены для оператора после отдельного согласования.

Путь: песочница/credential broker → общий Squid → назначенный группе HTTP
upstream. Группа OpenAI в пилоте использует host bridge listener → существующий
`127.0.0.1:10809` Xray. Другим группам можно назначить другие HTTP proxy URL. Остальные hostname
Squid обслуживает напрямую. Один общий relay на host сохраняет текущий Xray
inbound и его проверенный маршрут; отдельные proxy-контейнеры не нужны.
Политика host-wide: группы и их назначения одинаковы для всех песочниц этого host.
`small=512m`, `max_active_sandboxes=3` в Pulsy сохраняются.

## Контракт для отдельной задачи Pulsy

Предлагаемые **новые** настройки приложения (в текущем Pulsy их не добавляли):

```dotenv
PULSY_CODEX_EGRESS_UPSTREAM_ROUTES=[{"proxy_url":"http://host.docker.internal:10810","destination_hosts":["api.openai.com","chatgpt.com"]}]
```

Pulsy разбирает JSON-массив групп и конструирует типизированный tuple:

```python
from soveren_agent_platform.sandbox import SandboxEgressUpstream

egress_upstreams = tuple(
    SandboxEgressUpstream(
        proxy_url=group.proxy_url,
        destination_hosts=tuple(group.destination_hosts),
    )
    for group in settings.codex_egress_upstream_routes
)
# В существующем configure_sandboxed_codex:
# resources="small", max_active_sandboxes=3, egress_upstreams=egress_upstreams
```

Настройка отсутствует/пустая либо `[]` — `egress_upstreams=()` и прямой трафик.
Каждой группе обязательны `proxy_url` и непустой `destination_hosts`; невалидная
группа или повтор hostname после нормализации — ошибка bootstrap. Повторы
запрещены и внутри группы, и между группами, даже с одинаковым proxy URL.
Разные hostname с одним proxy URL объединяются в один Squid peer. Максимум —
16 входных групп и 256 hostname суммарно. Никакого default на OpenAI в платформе
нет. Пример выше выбирает только два конкретных hostname: он не
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

Helper обновляет только собственную цепочку, не очищает host firewall и не
трогает conversation `DOCKER-USER`/`INPUT` drop rules. Обновление цепочки
выполняется одним `iptables-restore` commit. При отсутствии Squid или неверной
метадате unit не откроет listener.

```bash
sudo tee /usr/local/sbin/soveren-xray-firewall >/dev/null <<'SH'
#!/bin/bash
set -euo pipefail
bridge_gateway="$1"
proxy_ip=$(docker inspect soveren-sandbox-egress --format \
  '{{with index .NetworkSettings.Networks "soveren-sandbox-public-egress"}}{{.IPAddress}}{{end}}')
proxy_mac=$(docker inspect soveren-sandbox-egress --format \
  '{{with index .NetworkSettings.Networks "soveren-sandbox-public-egress"}}{{.MacAddress}}{{end}}')
python3 - "$bridge_gateway" "$proxy_ip" "$proxy_mac" <<'PY'
import ipaddress, re, sys
for text in sys.argv[1:3]:
    value = ipaddress.ip_address(text)
    assert value.version == 4 and value.is_private
assert re.fullmatch(r"(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}", sys.argv[3])
PY
iptables-restore --wait 5 --noflush <<RULES
*filter
:SOVEREN-XRAY - [0:0]
-F SOVEREN-XRAY
-A SOVEREN-XRAY -s ${proxy_ip}/32 -m mac --mac-source ${proxy_mac} -j ACCEPT
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
sudo systemctl enable --now soveren-xray-bridge.socket
sudo systemctl is-active soveren-xray-firewall.service soveren-xray-bridge.socket
sudo iptables -S SOVEREN-XRAY
sudo ss -ltnp 'sport = :10810'
```

Не публиковать port через Docker и не менять Xray listen на `0.0.0.0`.
Установленный firewall helper заново выполняется при boot через dependency
socket unit. Если Docker ещё не восстановил Squid, listener не поднимется:
после появления контейнера перезапустить firewall unit и socket. При изменении
bridge gateway необходимо обновить оба unit-файла перед продолжением.

### 3. После согласованных release и отдельного обновления Pulsy

Остановить приём новой работы и активные sandbox turns штатным shutdown Pulsy.
Обновить package/images и настройки Pulsy. При первом acquire manager заменит
общий Squid, сохранив sandbox workspace и сетевые drop rules. После замены
его IP/MAC могут измениться: обязательно повторить

```bash
sudo systemctl restart soveren-xray-firewall.service
sudo docker exec soveren-sandbox-egress getent ahostsv4 host.docker.internal
sudo docker exec soveren-sandbox-egress squid -k parse -f /run/soveren-squid.conf
sudo docker exec soveren-sandbox-egress cat /run/soveren-squid.conf
```

Адрес `host.docker.internal` должен совпадать с `egress_host_gateway`.
Если Docker переопределяет `host-gateway`, исправить явную конфигурацию
listener/upstream, не расширять firewall allowlist. Старая IP/MAC пара не даёт
новому Squid доступ; до обновления helper selected-запросы падают закрыто.
Не разрешать повторное использование старой сетевой идентичности контейнера.

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
Это не live-проверка Xray или полноценной песочницы на Pulsy host. Host-side
units/firewall выше требуют операционной проверки в пилоте, включая boot и
recreation Squid; код host из этой задачи не запускался.

Основные механизмы: [Squid never_direct](https://www.squid-cache.org/Doc/config/never_direct/),
[cache_peer_access](https://www.squid-cache.org/Doc/config/cache_peer_access/),
[Docker host-gateway](https://docs.docker.com/reference/cli/docker/container/run/#add-entries-to-container-hosts-file---add-host),
[systemd socket-proxyd](https://github.com/systemd/systemd/blob/main/man/systemd-socket-proxyd.xml).
