# Парсер json-логов
Модуль сделан для преобразования json-логов в "плоский" табличный вид.
Также в библиотеке содежится консольная утилита для быстрой трансформации json файлов.
 
##Пример работы jflat
###test.json
```json
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      },
      {
        "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      },
      {
        "category": "fiction",
        "author": "J. R. R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  },
  "expensive": 10,
}
```
###Результат
```json lines
{"store_book_category":"reference","store_book_author":"Nigel Rees","store_book_title":"Sayings of the Century","store_book_price":8.95,"store_bicycle_color":"red","store_bicycle_price":19.95,"expensive":10}
{"store_book_category":"fiction","store_book_author":"Evelyn Waugh","store_book_title":"Sword of Honour","store_book_price":12.99,"store_bicycle_color":"red","store_bicycle_price":19.95,"expensive":10}
{"store_book_category":"fiction","store_book_author":"Herman Melville","store_book_title":"Moby Dick","store_book_price":8.99,"store_book_isbn":"0-553-21311-3","store_bicycle_color":"red","store_bicycle_price":19.95,"expensive":10}
{"store_book_category":"fiction","store_book_author":"J. R. R. Tolkien","store_book_title":"The Lord of the Rings","store_book_price":22.99,"store_book_isbn":"0-395-19395-8","store_bicycle_color":"red","store_bicycle_price":19.95,"expensive":10}
```

## Встраивание в java приложение
Репозиторий maven http://dpnexus.ftc.ru/repository/libs-release-local/:
```
// добавление в gradle
repositories {
    maven{
        url = 'http://dpnexus.ftc.ru/repository/libs-release-local/'
    }
}
dependencies {
    compile 'ru.ftc.utils:jflat:version'
}
```

`Transformer` - можно использовать для встраивания в любое java приложение.
Метод `transform` принимает json-документ в виде строки и в ответ возвращает список "плоских" JSON строк. Для работы ему потребуется схема разбора документа.

## Схема разбора документа
Схема определяет какие данне из документа требуется вывести в результат, а также она предоставляет возможность задать параметры обработки конкретных узлов.
Есть несколько способов определения схемы, самый простой из них: задать схему в виде списка путей имен требующихся в результате. При этом, вложенные пути нужно будет описать составными именами через символ подчеркивания '_'. Также при описании имен поддерживается wildcard при помощи символов '*' и '?''.
При парсинге вложенных массивов порядок следованя элементов сохраняется.
Например, если для документа выше задать схему вида:
* store_book_title
* store_book_price
* store_bicycle*

то результатом работы парсера будут документы: 
```json lines
{"store_book_title":"Sayings of the Century","store_book_price":8.95,"store_bicycle_color":"red","store_bicycle_price":19.95}
{"store_book_title":"Sword of Honour","store_book_price":12.99,"store_bicycle_color":"red","store_bicycle_price":19.95}, {"store_book_title":"Moby Dick","store_book_price":8.99,"store_bicycle_color":"red","store_bicycle_price":19.95}
{"store_book_title":"The Lord of the Rings","store_book_price":22.99,"store_bicycle_color":"red","store_bicycle_price":19.95}
```
Java код реализующий такое преобразование:
```java
        Schema schema = AutoSchemaFactory.builder()
                .columnStringFilters(Arrays.asList("store_book_title", "store_book_price", "store_bicycle*"))
                .build()
                .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
```

### Автоматическое построение схемы
`AutoSchemaFactory` строит схему обработку документа автоматически, на основании входящего документа.
Кроме того, `AutoSchemaFactory` позволяет определить:
* [JsonPath](https://github.com/json-path/JsonPath) для фильтрации json строк которые следует обрабатывать (по умолчанию фильтрация выключена)
* разделитель, при помощи которого будут формироваться имена полей (по умолчанию '_')
* политику группировки массивов примитивных типов (по умолчанию ARRAY)
* политику группировик массивов структур (по умолчанию NO_GROUP)

Реализованы следующие политики обработки вложенных json массивов:
  * CONCAT – конкатенация значений в строку через запятую ',';
  * ARRAY – оставить как массив в результате
  * COLUMNS – каждый элемент массива будет преобразован в отдельное поле в результирующем json c именем индекса массива
  * NO_GROUP – не группировать элементы массива, каждому элементу будет соответвовать отдельный документ в результирующем массиве

Например, если задать политику обработки массивов структур как `COLUMNS`
```java
        Schema schema = AutoSchemaFactory.builder()
                .columnStringFilters(Arrays.asList("store_book_title", "store_book_price", "store_bicycle*"))
                .complexArraysGroup(Schema.GroupPolicy.COLUMNS)
                .build()
                .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
```
то результатом будет:
```json lines
{"store_book_0_title":"Sayings of the Century","store_book_0_price":8.95,"store_book_1_title":"Sword of Honour","store_book_1_price":12.99,"store_book_2_title":"Moby Dick","store_book_2_price":8.99,"store_book_3_title":"The Lord of the Rings","store_book_3_price":22.99,"store_bicycle_color":"red","store_bicycle_price":19.95}```
```
С другими примерами задания схем через внешний JSON файл можно ознакомиться в тестовых классах: `AutoSchemaTest` и `AutoSchemaEveryLineTest` 

### Определение схемы через внешний JSON файл
`JsonSchemaFactory` задает схему разбора документа на основе внешнего json файла. В описании схемы необходимо будет описать вложенность структур исходного json файла и есть возможность описать правила преобразования каждого узла.
В схеме определюятся:
- `name` – строка, название схемы, не влияет разбор документа
- `version` – строка, версия схемы, не влияет разбор документа
- `filter` –  объект для фильтрации json строк которые следует обрабатывать (по умолчанию фильтрация выключена)
    - `path` - [JsonPath](https://github.com/json-path/JsonPath), на основании которого входящий документ будет или не будет обрабатываться
    - `class` - класс фильтрации, должен наследовать интерфейс `Filter`
      - `Exist` - если описанный JsonPath сущетвует, то документ будет обработан
      - `NotExist` - если описанный JsonPath сущетвует, то документ будет пропущен 
- `columns` - массив объектов, список колонок. Колонки имеют иерархическую структуру. В итоговую таблицу будут выведены только листовые колонки
  - `name` - строка имя колонки, используется для именования полей в результирующем документе
  - `path` - строка JsonPath для получения значение колонки из документа. Путь всегда указывается относительно родительской ноды. Необязательное поле, если отсутствует, то в качестве пути будет использовано значение из поля name. Важное замечание: если элемент содержит массив объектов, то следует указать явно через JsonPath (например, выбор всех объектов массива '[*]') 
  - `fullname` - boolean, определяет правило именования колонки в итоговой таблице.
    - `false` – имя колонки будет сформировано конкатенацией имён всех родительских columns, в естественном порядке (используется по умолчанию) 
    - `true`, имя колонки будет совпадат со значением в name. 
  - `skipJsonIfEmpty` - boolean, позволяет пропустить преобразование документа, если в нем нет такого поля. 
    - `true` – пропущенный документ не будет выведен в итоговую таблицу. 
    - `false` – при отсутвии поля документ будет присутвовать в итоговой таблице без указаного поля (используется по умолчанию)
  - `skipRowIfEmpty` - boolean, пропустить данную строку итоговой таблицы, если после конвертации поля его значиние принимает значение null. По сути гарантирует, что все строки в результате будут содержать указанную колонку.
    - `true` – если в итоговой таблице поле имеет значение null, то не выводить строку в итоговую таблицу
    - `false` – если в итоговой таблице поле имеет значение null, то вывести строку в итоговую таблицу без этого поля (используется по умолчанию)
  - `group` - enum, политика объединения значений поля, если их несколько (см. политики обработки массивов). По умолчанию NO_GROUP. 
  - `converter` - объект, правило конвертации значения json при записи в таблицу, если требуется. Необязательное поле, по умолчанию тип колонки в итоговой таблице совпадет с типом значения в json.
    - `class` - строка, класс осуществляющий конвертацию, должен наследовать интерфейс `Converter`
      - `ToDatetime` – преобразование в формат yyyy-MM-dd'T'HH:mm:ss из формата UNIX milliseconds
        - `pattern` - паттерн даты
      - `ToLong` - преобразование в Long число
      - `ToString` – преобразование в String
  - `columns` - массив объектов column, список вложенных колонок.
  
### Пример описания JSON схемы 
Ниже приведена схема, которая выводит в результат только те строки у которых есть поле "isbn", а также переопределяет наименование цен книг с полного пути "store_book_price" на просто "price", причем значение поля преобразовано в строку.
```json
{
  "name": "Example schema",
  "version": "1.0",
  "columns": [
    {
      "name": "store",
      "columns": [
        {
          "name": "book",
          "columns": [
            {"name": "type", "path": "category"},
            {"name": "author"},
            {"name": "title"},
            {"name": "isbn", "skipRowIfEmpty": true},
            {
              "name": "price",
              "fullname": true,
              "converter": {
                "class": "ToString"
              }
            }
          ]
        }
      ]
    },
    {
      "name": "expensive"
    }
  ]
}
```
Результатом работы парсера будет список строк:
```json lines
{"store_book_type":"fiction","store_book_author":"Herman Melville","store_book_title":"Moby Dick","store_book_isbn":"0-553-21311-3","price":"8.99","expensive":10}
{"store_book_type":"fiction","store_book_author":"J. R. R. Tolkien","store_book_title":"The Lord of the Rings","store_book_isbn":"0-395-19395-8","price":"22.99","expensive":10}
```

С другими примерами задания схем через внешний JSON файл можно ознакомиться в тестовом классе: `JsonSchemaTest`

### Определение схемы в коде приложения
Схему, описанную в параграфе выше, можно определить при помощи кода:
```java
		Schema schema = new Schema();
        schema.setColumns(
        Arrays.asList(
        new Schema.Column("store", "store", Arrays.asList(
        new Schema.Column("book", "book[*]",
        Arrays.asList(
        new Schema.Column("type", "category", Collections.emptyList(), schema),
        new Schema.Column("author", "author", Collections.emptyList(), schema),
        new Schema.Column("title", "title", Collections.emptyList(), schema),
        new Schema.Column("isbn", "isbn", Collections.emptyList(), Schema.GroupPolicy.NO_GROUP, false, false, true, Converter.DEFAULT, schema),
        new Schema.Column("price", "price", Collections.emptyList(), Schema.GroupPolicy.NO_GROUP, true, false, false, new ToString(), schema)
        ),
        Schema.GroupPolicy.NO_GROUP, schema
        ),
        new Schema.Column("expensive", "expensive", Collections.emptyList(), schema)
        ), schema)
        )
        );
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
```
Результатом работы парсера будет список строк:
```json lines
{"store_book_type":"fiction","store_book_author":"Herman Melville","store_book_title":"Moby Dick","store_book_isbn":"0-553-21311-3","price":"8.99","expensive":10}
{"store_book_type":"fiction","store_book_author":"J. R. R. Tolkien","store_book_title":"The Lord of the Rings","store_book_isbn":"0-395-19395-8","price":"22.99","expensive":10}
```
## Работа с командной строкой
За работу с командной строкой отвечает класс `App`
Для его использования необходимо создать bash файл
```shell
java -jar "$( dirname -- "${BASH_SOURCE[0]}" )"/jflat.jar "$@"
```
Утилита поддерживает прием и передачу данных через стандартный input/output потоки.
Также поддерживается работа с log файлами, каждая линяя которых будет обрезаться до первого символа `[` или `{`.
Поддерживается преобразование файла в csv формат
Аргументы:
* `-i` Путь до input файла, по умолчанию стандартный ввод
* `-o` Путь до output файла, по умолчанию стандартный вывод
* `-f` JSONPath для фильтрации документов перед процессингом, работает только если не задан параметр `-s`
* `-s` Путь до JSON файла со схемой парсинга. По умолчанию, схема будет сгенерирована на основании входящего JSON файла
* `-d` Разделитель для именования колонок файла, по умолчанию используется `_`
* `-e` Кодировка входного файла, по умолчанию `utf-8`
* `-n` Если не задан параметр `-s`, генерирует схему только на основе первой строки файла
* `-a` Разворачивает вложенные массивы из простых типов в строки. По умолчанию оставляет без изменений
* `-c` Разворачивает вложенные массивы структур в колонки. По умолчанию разворачивает в строки
* `-csv` Пишет результат в CSV формате. Разделитель `;`. Всегда генерирует схему по первому документу в файле, если не указан параметр `-s`
* `-h` Помощь

Все остальные параметры задают схему разбора документа, перечислением через пробел необходимых в результате колонок. Поддерживаются символы `*` и `?`. По умолчанию, схема будет сгенерирована на основании входящего JSON файла

Пример использования:
```shell
cat example.json > jflat store_book_title store_book_price store_bicycle*
{"store_book_title":"Sayings of the Century","store_book_price":8.95,"store_bicycle_color":"red","store_bicycle_price":19.95}
{"store_book_title":"Sword of Honour","store_book_price":12.99,"store_bicycle_color":"red","store_bicycle_price":19.95}, {"store_book_title":"Moby Dick","store_book_price":8.99,"store_bicycle_color":"red","store_bicycle_price":19.95}
{"store_book_title":"The Lord of the Rings","store_book_price":22.99,"store_bicycle_color":"red","store_bicycle_price":19.95}
```
        
        
