CREATE TABLE "users" (
    "id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    PRIMARY KEY ("id")
) USING memtx DISTRIBUTED BY ("id") OPTION (TIMEOUT = 3.0);

INSERT INTO "users" VALUES (1, 'Вася');
INSERT INTO "users" VALUES (2, 'Коля');
INSERT INTO "users" VALUES (3, 'Петя');
INSERT INTO "users" VALUES (4, 'Соня');
INSERT INTO "users" VALUES (5, 'Джон');
INSERT INTO "users" VALUES (6, 'Майкл');
INSERT INTO "users" VALUES (7, 'Сьюзан');
INSERT INTO "users" VALUES (8, 'Ольга');
INSERT INTO "users" VALUES (9, 'Кристина');
INSERT INTO "users" VALUES (10, 'Савелий');
