<?php


namespace rickcy\tarantool\tests\integration;


use Exception;
use PHPUnit\Framework\TestCase;
use rickcy\tarantool\Connection;
use rickcy\tarantool\tests\User;

class ConnectionTest extends TestCase
{
    /**
     * @throws Exception
     */
    public function testConnection(): void
    {
        $connection = new Connection();
        $connection->dsn = 'tcp://175.25.125.7:3301';
        $connection->username = 'tester';
        $connection->password = 'test';
        $connection->socket_timeout = 20000;
        $connection->connect_timeout = 20000;
        $connection->persistent = true;

//        $connection->evaluate(<<<LUA
//        if box.space[...] then box.space[...]:drop() end
//        space = box.schema.space.create(...)
//        space:format({
//        {name='id',type='string'},
//        {name='owner_id',type='string'},
//        {name='type',type='string'},
//        {name='status',type='string'},
//        {name='status_history',type='string'},
//        {name='params',type='string'}
//        })
//         space:create_index('primary', {type = 'tree', parts = {1, 'string'}})
//         space:create_index('secondary', {type = 'tree', parts = {2, 'string'}})
//        LUA
//            , 'announcements');

        $announcements = $connection->createCommand('SELECT * FROM "announcements"')->query();
        $this->assertTrue(true);


    }


    public function testActiveRecordInsert(): void
    {

        $container = \Yii::$container;
        $container->setDefinitions([
            'tarantool' =>
                [
                    'class' => Connection::class,
                    'dsn' => 'tcp://185.45.0.100:3301',
                    'username' => 'tester',
                    'password' => 'test',
                    'persistent' => true,
                ],
        ]);

        $user = new User();
        $user->guid = '1fb4154db-76b2-11e7-bc8d-901b0ebda105';
        $user->email = 'kuden.and.ko@gmail.com';
        $user->name = 'Евгений';
        $user->surname = 'Куденко';
        $user->middle_name = 'Сергеевич';

        $isSave = $user->save();
        $this->assertTrue($isSave);
    }


    public function testActiveRecordUpdate()
    {

        $container = \Yii::$container;
        $container->setDefinitions([
            'tarantool' =>
                [
                    'class' => Connection::class,
                    'dsn' => 'tcp://185.45.0.100:3301',
                    'username' => 'tester',
                    'password' => 'test',
                    'persistent' => true,
                ],
        ]);

        $user = User::findOne(['email' => 'kuden.and.ko@gmail.com']);
        $user->guid = '1fb4154db-76b2-11e7-bc8d-901b0ebda105';
        $user->email = '1ku-den@mail.ru';
        $user->name = 'Евгений';
        $user->surname = 'Куденко';
        $user->middle_name = 'Сергеевич';

        $isSave = $user->save();
        $this->assertTrue($isSave);
    }

}