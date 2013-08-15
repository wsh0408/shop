<?php
/*
 * 数据库适配器
 */
abstract  class db_Adapter{
    protected static $dbhost;
    protected static $dbport;
    protected static $dbname;
    protected static $user;
    protected static $passwd;
    public static $charset = 'utf8';
    public static $fetchstyle;
    public static $debug = false;
    protected static $logfile;
    
    protected static $link;   //保存数据库实例对象句柄
   
    public $lastInsertId = 0;  //最后一次插入的id;
    
    public $affectedRows = 0;  //影响的行数;
  
    protected $lastSql = '';    //最后执行的sql语句；
   
    protected $sqlStructure = array();   //待拼接的sql结构；
    
    protected $data = array();        //待更新的用户数据；
 
    

    
    public function __construct() {
        $cfg = Config::getIns();
        $config = $cfg->config; 
        self::$dbhost = $config['db']['host'];
        self::$user = $config['db']['user'];
        self::$passwd = $config['db']['password'];
        self::$dbport = $config['db']['port'];
        self::$dbname = $config['db']['dbname'];
        self:: $charset = $config['db']['charset'];
        self::$fetchstyle = $config['db']['fetchstyle'];
        self::$debug = $config['db']['debug'];
        self::$logfile  = $config['db']['logfile'];

        self::$link = $this->connect();
        $this->sqlStructure = $this->resetSqlStructure();
    }
    
    
    /*
     * 重置sql语句
     * @param void
     * @return bool
     */
    public function resetSqlStructure(){
        return array(
                'FIELDS' => '*',
                'FROM'   => '',
                'JOIN'   => '',
                'ON'     => '',
                'WHERE'  => '',
                'ORDER'  => '',
                'LIMIT'  => '',
                'OTHER'=>''
               );
   }
/*
 * 重新设置字符集；（数据库链接初始化时已经设置过。）
 */   
   
   public function setChar($charset= 'utf8'){
       $this->sqlStructure['OTHER'] = "SET NAMES {$charset}";
       $res = $this->query('','OTHER');
       return $res;
   }

   /*
    * 多样化sql语句拼接的方式。（可以使用一个方法的形式）
    */
   public function __call($methodName, $arg) {
        $tableName = $arg[0];
        $value = $arg[1];
        //getByXxx
        if (0 === strpos($methodName, 'getBy') && !empty($value)) {
            //取字段名:getByUserName =>user_name,getByPassword => password
            $field = strtolower(preg_replace('/(\w)([A-Z])/', '\\1_\\2', substr($methodName, 5)));
            if (is_array($value)) {
                $where = "`{$field}` IN('".join("','", $value)."')";
                return $this->where($where)->getAll($tableName);// 多条
            } elseif (is_scalar($value)) {
                $where = "`{$field}` = '{$value}'";
                return $this->where($where)->getRow($tableName);// 单条
            }
        }
      if(strtoupper($methodName) =='DATA'){
          $this->data = $value;
          return $this;
      }
      if(strtoupper($methodName) =='FIELDS' && is_array($value)){
          $this->sqlStructure['FIELDS'] = implode(',', $value);
      }
        //连贯操作
        if (array_key_exists(strtoupper($methodName), $this->sqlStructure)) {
            $this->sqlStructure[strtoupper($methodName)] = $value;
            return $this;
        }
        return "method '{$methodName}' not exists!"  ;
   }
      
/*
 * 返回结果集类的总方法。
 */
   public function select($tableName,$condition=''){
       if(!empty($condition)){
           $this->sqlStructure['WHERE'] = $condition;
       }
       $sql = $this->getSql($tableName,'select');
      $this->execute($sql);
       $res = $this->fetchAll(self::$fetchstyle);
       $this->sqlStructure = $this->resetSqlStructure();
       return $res;
   }
   
   /*
    * 拼接where条件。
    */
   protected function comWhere ($where){
        $tmp = array();
        $allowModes = array('=', '!=', '>=', '<=', '><', '>', '<', 'in','%like%');
        foreach ($where as $key => $value) {
            $mode = '=';
            if (preg_match('/\[(.*?)\]/', $key, $matchs)) {
                $key = strstr($key, '[', true); //去除[xxx]
                if (in_array($matchs[1], $allowModes)) {
                    $mode = $matchs[1];
                }
            }
            if ($mode == '><') {//between and
                if (is_array($value) && count($value) == 2) {
                    $value = array_values($value);
                    $tmp[] = "`{$key}` BETWEEN '{$value[0]}' AND '{$value[1]}";
                } 
            } else {
                $tmp[] = "`{$key}` {$mode} ('{$value}')";
            }
        }
        return join(' AND ', $tmp);
   }
   
/*
 * 拼接SQL。
 */
    protected function  getSql($tableName,$sqltype){
          if (is_array($this->sqlStructure['WHERE'])) {
            $this->sqlStructure['WHERE'] = $this->comWhere($this->sqlStructure['WHERE']);
        }  
        $fields = empty($this->sqlStructure['FIELDS'])? '*' : $this->sqlStructure['FIELDS'];
        $from   = empty($this->sqlStructure['FROM'])? " FROM `{$tableName}`": " FROM {$this->sqlStructure['FROM']}";
        $join   = empty($this->sqlStructure['JOIN'])? '' : "$tableName JOIN {$this->sqlStructure['JOIN']}";
        $on     = empty($this->sqlStructure['ON'])? '' :  " ON {$this->sqlStructure['ON']}";
        $where  = empty($this->sqlStructure['WHERE'])? '' : " WHERE {$this->sqlStructure['WHERE']}";
        $order  = empty($this->sqlStructure['ORDER'])? '' :  " ORDER BY {$this->sqlStructure['ORDER']}";
        $limit  = empty($this->sqlStructure['LIMIT'])? '': " LIMIT {$this->sqlStructure['LIMIT']}";
        $other  =  "{$this->sqlStructure['OTHER']}";
        $sql = '';
        if(!empty($this->data)){
        $this->data = $this->escape($this->data);
        }
        switch (strtolower($sqltype)) {
            case 'select':
                $sql = "SELECT {$fields}{$from}{$join}{$on}{$where}{$order}{$limit}";
                break;
            case 'delete':
                $sql = "DELETE {$from}{$join}{$on}{$where}{$order}{$limit}";
                break;
            case 'updata':
                if (empty($this->data)) {
                    return false;
                }
                $arr = array();
                foreach ($this->data as $field => $value) {
                    $arr[] = "`{$field}` = '{$value}'";
                }
                $setString = join(',', $arr);
                $sql = "UPDATE `{$tableName}` SET {$setString}{$where}{$order}{$limit} ";
                break;
            case 'insert':
                if (empty($this->data)) {
                    return false;
                }
                $keys   = join(',', array_keys($this->data));
                $value = '"' . join('","', $this->data) . '"';
                $sql    = "INSERT INTO `{$tableName}`({$keys}) VALUES({$value})";
                break;
            default:
                $sql =$other; 
                break;
        }
        $this->lastSql = $sql;
         return trim($sql);
   }
/*
 * 返回表中所有字段名组成的数组。
 */
   public function getFields($tableName){
       $this->sqlStructure['OTHER'] = "DESC {$tableName}";
      $sql = $this->getSql('', 'other');
       $this->execute($sql);
       $res  = $this->fetchAll(self::$fetchstyle);
       $this->sqlStructure = $this->resetSqlStructure();
       return $res;
   }
   
   /*
    * 返回数据库里所有表名组成的数组。
    */
   public function  getTables(){
       $this->sqlStructure['OTHER'] = "SHOW TABLES";
       $sql = $this->getSql('', 'other');
       $this->execute($sql);
       $res  = $this->fetchAll(self::$fetchstyle);
       $this->sqlStructure = $this->resetSqlStructure();
       return $res;
   }
   
   /*
    * select的别名方法。默认返回所有结果集。
    */
   public function getAll($tableName,$condition=''){
       $res = $this->select($tableName,$condition);
       return $res;
   }
   
   /*
    * 返回结果集的一条记录。
    */
   public function getRow($tableName,$condition=''){
       $this->sqlStructure['LIMIT'] = '1';
       $res = $this->select($tableName,$condition);
       return $res;
   }
   
   public function getCell($tableName,$field,$condition){
       $this->sqlStructure['FIELDS'] = $field;
       if(!empty($condition)){
       $this->sqlStructure['WHERE'] = $condition;
       }
       $sql = $this->getSql($tableName, 'select');
       $this->execute($sql);
       $res = $this->fetchOne(self::$fetchstyle);
       return $res[$field];
   }
   /*
    * 查询表行数。
    */
   public function getCount($tableName,$condition=''){
       $this->sqlStructure['FIELDS'] = 'COUNT(*)';
       $this->sqlStructure['WHERE'] = $condition;
       $res = $this->select($tableName);
          return $res[0]['COUNT(*)'];
   }
   
  /*
   * 判断数据是否存在。
   */ 
   public function has($tableName,$condition){
       $this->sqlStructure['FIELDS'] = '1';
       $res = $this->select($tableName,$condition);
       if($res){
           return 1 ;
       }  else {
           return 0;    
       }
       
   }
   /*
    * 返回一列数据。
    */
   public function getCol($tableName,$col,$condition=''){
        $arr =   $this->getFields($tableName);
        foreach ($arr as $value){
        $fields[] = array_shift($value);
        }
        if(in_array($col, $fields)){
            $this->sqlStructure['FIELDS'] = $col;
           $res = $this->select($tableName,$condition);
           foreach ($res as $key => $value) {
                   $field[] = $value[$col];
           }
           return $field;
        }  else {
            echo "字段名不存在";
        }
   }
   
   /*
    * 插入数据
    */
   public function insert($tableName,$data){
       $this->data = $data;
       $res = $this->query($tableName,'insert');
       return $res;
   }
   
   /*
    *删除数据。 
    */
   public function del($tableName,$condition){  
       $this->sqlStructure['WHERE'] = $condition;
       $res = $this->query($tableName,'delete');
   }
   
   /*
    * 更新数据。
    */
   public function updata($tableName,$data,$condition){
       $this->data = $data;
       $this->sqlStructure["WHERE"] = $condition;
       $res = $this->query($tableName,'updata');
       return $res;
   }
   
   /*
    * 转义数据。
    */
   protected function escape($arr) {
    foreach($arr as $k=>$v) {
        if(is_string($v)) {
            $arr[$k] = addslashes($v);
        } else if(is_array($v)) {  // 再加判断,如果是数组,调用自身,再转
            $arr[$k] = escape($v);
        }
    }
    return $arr;
}
   
   /*
         * 把错误记录到日志中。
         */
        protected function error2log($error) {
           $sql = $this->lastSql;
        if(!empty($sql)){
            $sql = $this->escape($sql);
        }
        if(!file_exists(self::$logfile)){
            file_put_contents(self::$logfile, $sql."\r\n".$error);
        }  else {
            file_put_contents(self::$logfile, "\r\n".$sql."\r\n".$error,FILE_APPEND);
        }
    }
    
   abstract protected function fetchAll($fetchstyle);
   
   abstract protected function fetchOne($fetchstyle);

   abstract protected function connect();
   
   abstract protected function getError($ExceptionObj);
   
   abstract protected function query($tableName,$sqltype);

   abstract protected function execute($sql);
   
   abstract protected function getInsertId();
   
   abstract protected function getAffectedRows();
   
   abstract public function beginTransaction();
    
   abstract public function commit();
   
   abstract public function rollback();
   
   abstract protected function close();
   
}
