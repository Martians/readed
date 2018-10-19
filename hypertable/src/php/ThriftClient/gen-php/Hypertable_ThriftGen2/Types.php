<?php
namespace Hypertable_ThriftGen2;

/**
 * Autogenerated by Thrift Compiler (1.0.0-dev)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
use Thrift\Base\TBase;
use Thrift\Type\TType;
use Thrift\Type\TMessageType;
use Thrift\Exception\TException;
use Thrift\Exception\TProtocolException;
use Thrift\Protocol\TProtocol;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\Exception\TApplicationException;


/**
 * Result type of HQL queries
 * 
 * <dl>
 *   <dt>results</dt>
 *   <dd>String results from metadata queries</dd>
 * 
 *   <dt>cells</dt>
 *   <dd>Resulting table cells of for buffered queries</dd>
 * 
 *   <dt>scanner</dt>
 *   <dd>Resulting scanner ID for unbuffered queries</dd>
 * 
 *   <dt>mutator</dt>
 *   <dd>Resulting mutator ID for unflushed modifying queries</dd>
 * </dl>
 */
class HqlResult {
  static $_TSPEC;

  /**
   * @var string[]
   */
  public $results = null;
  /**
   * @var \Hypertable_ThriftGen\Cell[]
   */
  public $cells = null;
  /**
   * @var int
   */
  public $scanner = null;
  /**
   * @var int
   */
  public $mutator = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'results',
          'type' => TType::LST,
          'etype' => TType::STRING,
          'elem' => array(
            'type' => TType::STRING,
            ),
          ),
        2 => array(
          'var' => 'cells',
          'type' => TType::LST,
          'etype' => TType::STRUCT,
          'elem' => array(
            'type' => TType::STRUCT,
            'class' => '\Hypertable_ThriftGen\Cell',
            ),
          ),
        3 => array(
          'var' => 'scanner',
          'type' => TType::I64,
          ),
        4 => array(
          'var' => 'mutator',
          'type' => TType::I64,
          ),
        );
    }
    if (is_array($vals)) {
      if (isset($vals['results'])) {
        $this->results = $vals['results'];
      }
      if (isset($vals['cells'])) {
        $this->cells = $vals['cells'];
      }
      if (isset($vals['scanner'])) {
        $this->scanner = $vals['scanner'];
      }
      if (isset($vals['mutator'])) {
        $this->mutator = $vals['mutator'];
      }
    }
  }

  public function getName() {
    return 'HqlResult';
  }

  public function read($input)
  {
    $xfer = 0;
    $fname = null;
    $ftype = 0;
    $fid = 0;
    $xfer += $input->readStructBegin($fname);
    while (true)
    {
      $xfer += $input->readFieldBegin($fname, $ftype, $fid);
      if ($ftype == TType::STOP) {
        break;
      }
      switch ($fid)
      {
        case 1:
          if ($ftype == TType::LST) {
            $this->results = array();
            $_size0 = 0;
            $_etype3 = 0;
            $xfer += $input->readListBegin($_etype3, $_size0);
            for ($_i4 = 0; $_i4 < $_size0; ++$_i4)
            {
              $elem5 = null;
              $xfer += $input->readString($elem5);
              $this->results []= $elem5;
            }
            $xfer += $input->readListEnd();
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 2:
          if ($ftype == TType::LST) {
            $this->cells = array();
            $_size6 = 0;
            $_etype9 = 0;
            $xfer += $input->readListBegin($_etype9, $_size6);
            for ($_i10 = 0; $_i10 < $_size6; ++$_i10)
            {
              $elem11 = null;
              $elem11 = new \Hypertable_ThriftGen\Cell();
              $xfer += $elem11->read($input);
              $this->cells []= $elem11;
            }
            $xfer += $input->readListEnd();
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 3:
          if ($ftype == TType::I64) {
            $xfer += $input->readI64($this->scanner);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 4:
          if ($ftype == TType::I64) {
            $xfer += $input->readI64($this->mutator);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        default:
          $xfer += $input->skip($ftype);
          break;
      }
      $xfer += $input->readFieldEnd();
    }
    $xfer += $input->readStructEnd();
    return $xfer;
  }

  public function write($output) {
    $xfer = 0;
    $xfer += $output->writeStructBegin('HqlResult');
    if ($this->results !== null) {
      if (!is_array($this->results)) {
        throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
      }
      $xfer += $output->writeFieldBegin('results', TType::LST, 1);
      {
        $output->writeListBegin(TType::STRING, count($this->results));
        {
          foreach ($this->results as $iter12)
          {
            $xfer += $output->writeString($iter12);
          }
        }
        $output->writeListEnd();
      }
      $xfer += $output->writeFieldEnd();
    }
    if ($this->cells !== null) {
      if (!is_array($this->cells)) {
        throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
      }
      $xfer += $output->writeFieldBegin('cells', TType::LST, 2);
      {
        $output->writeListBegin(TType::STRUCT, count($this->cells));
        {
          foreach ($this->cells as $iter13)
          {
            $xfer += $iter13->write($output);
          }
        }
        $output->writeListEnd();
      }
      $xfer += $output->writeFieldEnd();
    }
    if ($this->scanner !== null) {
      $xfer += $output->writeFieldBegin('scanner', TType::I64, 3);
      $xfer += $output->writeI64($this->scanner);
      $xfer += $output->writeFieldEnd();
    }
    if ($this->mutator !== null) {
      $xfer += $output->writeFieldBegin('mutator', TType::I64, 4);
      $xfer += $output->writeI64($this->mutator);
      $xfer += $output->writeFieldEnd();
    }
    $xfer += $output->writeFieldStop();
    $xfer += $output->writeStructEnd();
    return $xfer;
  }

}

/**
 * Same as HqlResult except with cell as array
 */
class HqlResult2 {
  static $_TSPEC;

  /**
   * @var string[]
   */
  public $results = null;
  /**
   * @var string[][]
   */
  public $cells = null;
  /**
   * @var int
   */
  public $scanner = null;
  /**
   * @var int
   */
  public $mutator = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'results',
          'type' => TType::LST,
          'etype' => TType::STRING,
          'elem' => array(
            'type' => TType::STRING,
            ),
          ),
        2 => array(
          'var' => 'cells',
          'type' => TType::LST,
          'etype' => TType::LST,
          'elem' => array(
            'type' => TType::LST,
            'etype' => TType::STRING,
            'elem' => array(
              'type' => TType::STRING,
              ),
            ),
          ),
        3 => array(
          'var' => 'scanner',
          'type' => TType::I64,
          ),
        4 => array(
          'var' => 'mutator',
          'type' => TType::I64,
          ),
        );
    }
    if (is_array($vals)) {
      if (isset($vals['results'])) {
        $this->results = $vals['results'];
      }
      if (isset($vals['cells'])) {
        $this->cells = $vals['cells'];
      }
      if (isset($vals['scanner'])) {
        $this->scanner = $vals['scanner'];
      }
      if (isset($vals['mutator'])) {
        $this->mutator = $vals['mutator'];
      }
    }
  }

  public function getName() {
    return 'HqlResult2';
  }

  public function read($input)
  {
    $xfer = 0;
    $fname = null;
    $ftype = 0;
    $fid = 0;
    $xfer += $input->readStructBegin($fname);
    while (true)
    {
      $xfer += $input->readFieldBegin($fname, $ftype, $fid);
      if ($ftype == TType::STOP) {
        break;
      }
      switch ($fid)
      {
        case 1:
          if ($ftype == TType::LST) {
            $this->results = array();
            $_size14 = 0;
            $_etype17 = 0;
            $xfer += $input->readListBegin($_etype17, $_size14);
            for ($_i18 = 0; $_i18 < $_size14; ++$_i18)
            {
              $elem19 = null;
              $xfer += $input->readString($elem19);
              $this->results []= $elem19;
            }
            $xfer += $input->readListEnd();
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 2:
          if ($ftype == TType::LST) {
            $this->cells = array();
            $_size20 = 0;
            $_etype23 = 0;
            $xfer += $input->readListBegin($_etype23, $_size20);
            for ($_i24 = 0; $_i24 < $_size20; ++$_i24)
            {
              $elem25 = null;
              $elem25 = array();
              $_size26 = 0;
              $_etype29 = 0;
              $xfer += $input->readListBegin($_etype29, $_size26);
              for ($_i30 = 0; $_i30 < $_size26; ++$_i30)
              {
                $elem31 = null;
                $xfer += $input->readString($elem31);
                $elem25 []= $elem31;
              }
              $xfer += $input->readListEnd();
              $this->cells []= $elem25;
            }
            $xfer += $input->readListEnd();
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 3:
          if ($ftype == TType::I64) {
            $xfer += $input->readI64($this->scanner);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 4:
          if ($ftype == TType::I64) {
            $xfer += $input->readI64($this->mutator);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        default:
          $xfer += $input->skip($ftype);
          break;
      }
      $xfer += $input->readFieldEnd();
    }
    $xfer += $input->readStructEnd();
    return $xfer;
  }

  public function write($output) {
    $xfer = 0;
    $xfer += $output->writeStructBegin('HqlResult2');
    if ($this->results !== null) {
      if (!is_array($this->results)) {
        throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
      }
      $xfer += $output->writeFieldBegin('results', TType::LST, 1);
      {
        $output->writeListBegin(TType::STRING, count($this->results));
        {
          foreach ($this->results as $iter32)
          {
            $xfer += $output->writeString($iter32);
          }
        }
        $output->writeListEnd();
      }
      $xfer += $output->writeFieldEnd();
    }
    if ($this->cells !== null) {
      if (!is_array($this->cells)) {
        throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
      }
      $xfer += $output->writeFieldBegin('cells', TType::LST, 2);
      {
        $output->writeListBegin(TType::LST, count($this->cells));
        {
          foreach ($this->cells as $iter33)
          {
            {
              $output->writeListBegin(TType::STRING, count($iter33));
              {
                foreach ($iter33 as $iter34)
                {
                  $xfer += $output->writeString($iter34);
                }
              }
              $output->writeListEnd();
            }
          }
        }
        $output->writeListEnd();
      }
      $xfer += $output->writeFieldEnd();
    }
    if ($this->scanner !== null) {
      $xfer += $output->writeFieldBegin('scanner', TType::I64, 3);
      $xfer += $output->writeI64($this->scanner);
      $xfer += $output->writeFieldEnd();
    }
    if ($this->mutator !== null) {
      $xfer += $output->writeFieldBegin('mutator', TType::I64, 4);
      $xfer += $output->writeI64($this->mutator);
      $xfer += $output->writeFieldEnd();
    }
    $xfer += $output->writeFieldStop();
    $xfer += $output->writeStructEnd();
    return $xfer;
  }

}

/**
 * Same as HqlResult except with cell as array
 */
class HqlResultAsArrays {
  static $_TSPEC;

  /**
   * @var string[]
   */
  public $results = null;
  /**
   * @var string[][]
   */
  public $cells = null;
  /**
   * @var int
   */
  public $scanner = null;
  /**
   * @var int
   */
  public $mutator = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'results',
          'type' => TType::LST,
          'etype' => TType::STRING,
          'elem' => array(
            'type' => TType::STRING,
            ),
          ),
        2 => array(
          'var' => 'cells',
          'type' => TType::LST,
          'etype' => TType::LST,
          'elem' => array(
            'type' => TType::LST,
            'etype' => TType::STRING,
            'elem' => array(
              'type' => TType::STRING,
              ),
            ),
          ),
        3 => array(
          'var' => 'scanner',
          'type' => TType::I64,
          ),
        4 => array(
          'var' => 'mutator',
          'type' => TType::I64,
          ),
        );
    }
    if (is_array($vals)) {
      if (isset($vals['results'])) {
        $this->results = $vals['results'];
      }
      if (isset($vals['cells'])) {
        $this->cells = $vals['cells'];
      }
      if (isset($vals['scanner'])) {
        $this->scanner = $vals['scanner'];
      }
      if (isset($vals['mutator'])) {
        $this->mutator = $vals['mutator'];
      }
    }
  }

  public function getName() {
    return 'HqlResultAsArrays';
  }

  public function read($input)
  {
    $xfer = 0;
    $fname = null;
    $ftype = 0;
    $fid = 0;
    $xfer += $input->readStructBegin($fname);
    while (true)
    {
      $xfer += $input->readFieldBegin($fname, $ftype, $fid);
      if ($ftype == TType::STOP) {
        break;
      }
      switch ($fid)
      {
        case 1:
          if ($ftype == TType::LST) {
            $this->results = array();
            $_size35 = 0;
            $_etype38 = 0;
            $xfer += $input->readListBegin($_etype38, $_size35);
            for ($_i39 = 0; $_i39 < $_size35; ++$_i39)
            {
              $elem40 = null;
              $xfer += $input->readString($elem40);
              $this->results []= $elem40;
            }
            $xfer += $input->readListEnd();
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 2:
          if ($ftype == TType::LST) {
            $this->cells = array();
            $_size41 = 0;
            $_etype44 = 0;
            $xfer += $input->readListBegin($_etype44, $_size41);
            for ($_i45 = 0; $_i45 < $_size41; ++$_i45)
            {
              $elem46 = null;
              $elem46 = array();
              $_size47 = 0;
              $_etype50 = 0;
              $xfer += $input->readListBegin($_etype50, $_size47);
              for ($_i51 = 0; $_i51 < $_size47; ++$_i51)
              {
                $elem52 = null;
                $xfer += $input->readString($elem52);
                $elem46 []= $elem52;
              }
              $xfer += $input->readListEnd();
              $this->cells []= $elem46;
            }
            $xfer += $input->readListEnd();
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 3:
          if ($ftype == TType::I64) {
            $xfer += $input->readI64($this->scanner);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        case 4:
          if ($ftype == TType::I64) {
            $xfer += $input->readI64($this->mutator);
          } else {
            $xfer += $input->skip($ftype);
          }
          break;
        default:
          $xfer += $input->skip($ftype);
          break;
      }
      $xfer += $input->readFieldEnd();
    }
    $xfer += $input->readStructEnd();
    return $xfer;
  }

  public function write($output) {
    $xfer = 0;
    $xfer += $output->writeStructBegin('HqlResultAsArrays');
    if ($this->results !== null) {
      if (!is_array($this->results)) {
        throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
      }
      $xfer += $output->writeFieldBegin('results', TType::LST, 1);
      {
        $output->writeListBegin(TType::STRING, count($this->results));
        {
          foreach ($this->results as $iter53)
          {
            $xfer += $output->writeString($iter53);
          }
        }
        $output->writeListEnd();
      }
      $xfer += $output->writeFieldEnd();
    }
    if ($this->cells !== null) {
      if (!is_array($this->cells)) {
        throw new TProtocolException('Bad type in structure.', TProtocolException::INVALID_DATA);
      }
      $xfer += $output->writeFieldBegin('cells', TType::LST, 2);
      {
        $output->writeListBegin(TType::LST, count($this->cells));
        {
          foreach ($this->cells as $iter54)
          {
            {
              $output->writeListBegin(TType::STRING, count($iter54));
              {
                foreach ($iter54 as $iter55)
                {
                  $xfer += $output->writeString($iter55);
                }
              }
              $output->writeListEnd();
            }
          }
        }
        $output->writeListEnd();
      }
      $xfer += $output->writeFieldEnd();
    }
    if ($this->scanner !== null) {
      $xfer += $output->writeFieldBegin('scanner', TType::I64, 3);
      $xfer += $output->writeI64($this->scanner);
      $xfer += $output->writeFieldEnd();
    }
    if ($this->mutator !== null) {
      $xfer += $output->writeFieldBegin('mutator', TType::I64, 4);
      $xfer += $output->writeI64($this->mutator);
      $xfer += $output->writeFieldEnd();
    }
    $xfer += $output->writeFieldStop();
    $xfer += $output->writeStructEnd();
    return $xfer;
  }

}


