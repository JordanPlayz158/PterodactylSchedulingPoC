<?php

namespace Pterodactyl\Repositories\Wings;

use GuzzleHttp\Psr7\Response;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Webmozart\Assert\Assert;
use Pterodactyl\Models\Server;
use Psr\Http\Message\ResponseInterface;
use GuzzleHttp\Exception\TransferException;
use Pterodactyl\Exceptions\Http\Connection\DaemonConnectionException;

class DaemonPowerRepository extends DaemonRepository
{
    /**
     * Sends a power action to the server instance.
     *
     * @throws \Pterodactyl\Exceptions\Http\Connection\DaemonConnectionException
     */
    public function send(string $action, bool $force = false): ResponseInterface
    {
        $server = $this->server;
        Assert::isInstanceOf($server, Server::class);

        /* On the unlikely but possible condition that someone
         *  tries to change the power state of the server during
         *  a schedule relying on receiving the power state it
         *  expects, would use websocket to ensure it is not missed
         *  but then would need to add a new dependency and
         *  while these two seem fine, they.... might be abandoned?
         *  https://github.com/arthurkushman/php-wss
         *  https://github.com/Textalk/websocket-php
         */
        $uuid = $server->uuid;
        $lock = Cache::lock($uuid);

        if (!$force && !$lock->get()) {
            // I think wings sends the power exclusive lock error
            //  via WebSocket so don't got a way to inform the user
            //  via the same way as an actual lock error which is unfortunate
            Log::channel('job')->info('[DaemonPowerRepository] Lock could NOT be obtained and force was not true', ['server' => $uuid]);
            return new Response(204);
        }

        Log::channel('job')->info('[DaemonPowerRepository] Lock was locked or force was true',
            ['server' => $uuid, 'lock' => $lock, 'force' => $force]);

        try {
            return $this->getHttpClient()->post(
                sprintf('/api/servers/%s/power', $uuid),
                ['json' => ['action' => $action]]
            );
        } catch (TransferException $exception) {
            throw new DaemonConnectionException($exception);
        } finally {
            $lock->release();

            if (!$force) {
                Log::channel('job')->info('[DaemonPowerRepository] Lock was released', ['server' => $uuid, 'lock' => $lock]);
            }
        }
    }
}
