<?php


namespace rickcy\tarantool\tests\unit;


use PHPUnit\Framework\TestCase;
use rickcy\tarantool\Connection;
use rickcy\tarantool\tests\User;
use Tarantool\Client\Schema\Criteria;

class ConnectionTest extends TestCase
{
    /**
     * @throws \Exception
     */
    public function testConnection()
    {
        $connection = new Connection();
        $connection->dsn = 'tcp://175.25.125.7:3301';
        $connection->username = 'tester';
        $connection->password = 'test';
        $connection->socket_timeout = 20000;
        $connection->connect_timeout = 20000;
        $connection->persistent = true;

        $user = $connection->getSpace('USERS')->select(Criteria::key([21]))[0];
//        $connection->createCommand('CREATE TABLE users ("id" INTEGER PRIMARY KEY AUTOINCREMENT,"guid" VARCHAR(255), "email" VARCHAR(255),"name" VARCHAR(255),"surname" VARCHAR(255),"middle_name" VARCHAR(255))')->execute();

        $this->assertTrue(true);


    }


    public function testActiveRecordInsert()
    {

        $container = \Yii::$container;
        $container->setDefinitions([
            'tarantool' =>
                [
                    'class' => Connection::class,
                    'dsn' => 'tcp://175.25.125.7:3301',
                    'username' => 'tester',
                    'password' => 'test',
                    'persistent' => true,
                ],
        ]);

        $user = new User();
        $user->guid = '1fb4154db-76b2-11e7-bc8d-901b0ebda105';
        $user->email = '1kuden.and.ko@gmail.com';
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
                    'dsn' => 'tcp://175.25.125.7:3301',
                    'username' => 'tester',
                    'password' => 'test',
                    'persistent' => true,
                ],
        ]);

        $user = User::findOne(['id' => 21]);
        $user->guid = '1fb4154db-76b2-11e7-bc8d-901b0ebda105';
        $user->email = '1ku-den@mail.ru';
        $user->name = 'Евгений';
        $user->surname = 'Куденко';
        $user->middle_name = 'Сергеевич';

        $isSave = $user->save();
        $this->assertTrue($isSave);
    }

}