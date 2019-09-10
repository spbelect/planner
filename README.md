# Churo

Скачивает, склеивает, экспортирует.

Главный скрипт - `taskloop.py` - считывает таски из заданной директории и исполняет.
В директории каждый таск должен быть представлен json-файлом с параметрами. Пример в 
папке `sample`. Скрипт обнофляет json-файл, записывая статус running/finished/failed.

Каждый таск имеет тип: download/merge/export.

Пример использования: `./taskloop.py download` - исполняет таски c типом "download".

Скрипт и все его команды имеют справку с описанием параметров: `./taskloop.py download --help`

## Установка

```
pip install -r requirements.txt
```

## Настройки

Сначала скрипт проверяет параметры переданные в виде аргументоа при запуске.
Если нет аргумента, использует значение прописанное в файле `env-local`. См. пример
в файле `env-local.example`. Если нет в файле, считывает значение из переменной 
окружения (os.environ).

```
./taskloop.py 
Usage: taskloop.py [OPTIONS] COMMAND [ARGS]...

  Manage tasks.

Options:
  -l, --loglevel [CRITICAL|FATAL|ERROR|WARN|WARNING|INFO|DEBUG|NOTSET]
  --tasks-dir PATH                Директория в которой нахдятся json-файлы
                                  описывающие таски.  [required]
  --max-download-retries INTEGER
  --downloaded-dir PATH           Директория куда помещаются все скачанные
                                  сегменты.  [required]
  --merged-dir PATH               Директория куда помещаются все склееные
                                  видео.  [required]
  --help                          Show this message and exit.

Commands:
  download      Process download tasks from tasks dir.
  export        Process export tasks from tasks dir.
  fail-running  Mark all "running" tasks as "failed".
  merge         Process merge tasks from tasks dir.

```


## Download

```
./taskloop.py download --help
Usage: taskloop.py download [OPTIONS]

  Process download tasks from tasks dir.

Options:
  --num-download-workers INTEGER
  --tmp-download-dir PATH         [required]
  -f, --force / --no-force        Обнулить счетчик попыток и качать заново.
  --restart-finished              Перезапускать успешно завершенные ранее
                                  таски.
  --restart-failed                Перезапускать неуспешно завершенные ранее
                                  таски.
  --help                          Show this message and exit.
```

Каждый download таск:

    Скачать 15-минутный сегмент камеры camid, начинающийся со времени timestart во 
    временную папку, проверить на разрывы. 
    Если разрывов меньше чем в существующем файле, перезаписать его.
    Если есть разрывы - повторять скачивание max-download-retries раз.
    Сохранить отчет о разрывах в {segmentvideofile}.flv.gapreport.json
    Если force == False то продолжаем считать кол-во попыток с прошлого запуска.
    Если force == True то кол-во попыток с прошлого запуска обнуляется.
    

## Merge

```
./taskloop.py merge --help
Usage: taskloop.py merge [OPTIONS]

  Process merge tasks from tasks dir.

Options:
  --num-merge-workers INTEGER
  --tmp-merge-dir PATH            [required]
  --merge-08-20-tolerate-gaps-duration INTEGER
                                  Суммарное кол-во отсутствующих секунд с 8ч
                                  до 20ч допустимое для склеивания.
  --merge-tolerate-incomplete-downloads
                                  Допускать склеивание недокачанных сегментов
                                  с разрывами.
  -f, --force / --no-force        Перезаписать файл если существует.
  --restart-finished              Перезапускать успешно завершенные ранее
                                  таски.
  --restart-failed                Перезапускать неуспешно завершенные ранее
                                  таски.
  --help                          Show this message and exit.
```

Каждый merege таск:

    Проверить отчет о разрывах и склеить все файлы в папке в один временный файл.
    По окончании перенести временный файл в конечный.
    
    
## Export

Таск тупо исполняет shell-команду прописаную в `cmd` json-файла. Теоретически там должен быть вызов rsync копирующий из merged dir на другой сервер: 

```
rsync --rsh='ssh -oBatchMode=yes -p22' --recursive /tmp/churo/merged/ u1@localhost:/tmp/churo/exported
```

Пример см в папке `sample`.


# Planner

Отдельно от taskloop можно использовать planner - высокоуровневой скрипт. Принимает задания на выкачивание или склеивание диапазона УИК в заданном регионе. Имеет консольное и веб-api.

Документация веб-апи доступна после запуска скрипта в формате swagger по адресу http://127.0.0.1:8000/docs

```
./planner.py 
Usage: planner.py [OPTIONS] COMMAND [ARGS]...

  Manage plans.

Options:
  --elect-date [%Y-%m-%d]         Дата проведения выборов формата YYYY-MM-DD
                                  [required]
  -l, --loglevel [CRITICAL|FATAL|ERROR|WARN|WARNING|INFO|DEBUG|NOTSET]
  --help                          Show this message and exit.

Commands:
  add      Add plan.
  restart  Set plan "finished" = false.
  rm       Delete plan.
  run      Process all unfinished plans.
  show     Print current plans.
  uiks     Download or merge uiks.
```
