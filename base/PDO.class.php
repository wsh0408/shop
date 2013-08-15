<?php
class db_PDO extends db_Adapter{
  public static $stmt;//结果集对象
    /*
     * 连接动作
     */
    public function connect() {
        try {
             $dbh = new PDO('mysql:host='.self::$dbhost.';port=' .self::$dbport .';dbname=' .self::$dbname ,  self::$user,  self::$passwd,array(PDO::ATTR_DEFAULT_FETCH_MODE=>  self::$fetchstyle));
             $dbh->exec("set names ".self::$charset);
             if(self::$debug) $dbh->setAttribute (PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e) {
            $this->error2log($this->getError($e));
            exit( $e->getMessage());
        }
        return $dbh;
   }
   
   /*
    * 获取错误
    */
    public function getError($ExceptionObj){
        return "time :".date("Y-m-d H:i:s")."---File :".$ExceptionObj->getFile()."---Line : ".$ExceptionObj->getLine()."---Code :".$ExceptionObj->getCode()."---Message :".$ExceptionObj->getMessage();
        }
        

    /*
    * 执行一条不返回结果集的sql语句。
    */
        public function query($tableName,$sqltype) {
            try{
            $sql =  $this->getSql($tableName,$sqltype);
            $res = self::$link->exec($sql);
            }  catch (PDOException $e){
                $this->error2log($this->getError($e));
                exit( $e->getMessage());
            }
            return $res;
        }
        /*
         * 执行一个sql语句并返回结果集对象
         */
        public function execute($sql){
            try{
            self::$stmt = $stmt = self::$link->prepare($sql);
            $stmt->execute();
            }  catch (PDOException $e){
                $this->error2log($this->getError($e));
                exit( $e->getMessage());
            }
        }
        /*
    * 遍历结果集对象，得到遍历后的数组。
    */
        public function fetchAll($fetchstyle) {
            try{
            $res = self::$stmt->fetchAll($fetchstyle);
            }  catch (PDOException $e){
                      $this->error2log($this->getError($e));
                      exit( $e->getMessage());
            }
            return $res;
        }
        
        public function fetchOne($fetchstyle) {
            try{
                $res = self::$stmt->fetch($fetchstyle);
            }  catch (PDOException $e){
                  $this->error2log($this->getError($e));
                  exit( $e->getMessage());
            }
            return $res;
        }
        /*
    * 得到上次insert操作的最后ID
    */
        public function getInsertId(){
            $this->lastInsertId = self::$link->lastInsertID;
            return $this->lastInsertId;
        }
   /*
    * 得到最后一次insert,delete,updata等操作后，影响的行数。
    */
        public function getAffectedRows(){
            $this->affectedRows = self::$stmt->rowCount;
            return $this->affectedRows;
        }
   
/*
 * 事物。
 */
        public function beginTransaction(){
            self::$link->beginTransaction();
        }
    
        public function commit(){
            self::$link ->commit;
        }
   
        public function rollback(){
            self::$link->rollback;
        }
   
        public function close(){
            self::$stmt = null;
            self::$link = null;
        }
}
