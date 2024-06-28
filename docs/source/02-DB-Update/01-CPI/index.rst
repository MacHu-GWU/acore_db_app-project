CPI (Consumer Price Index)
==============================================================================


Overview
------------------------------------------------------------------------------
单机游戏中由于玩家数量很少, 没有健康的经济系统, 所以玩家打到的材料自己如果用不上只能低价卖 NPC, 自己如果需要的材料也无法从 AH 买到. 这就逼着玩家用 GM 命令从而破坏了游戏平衡性, 降低了游戏体验.

于是我就参考了 `Warmane <https://www.warmane.com/>`_ Icecrown 服务器的物价 (该服务器已经稳定运行十多年, 物价也一直稳定), 设计了常用物品的买卖价格, 并将新的价格更新到数据库中.

.. note::

    我曾经想开发一个 AH Bot 程序来直接对数据库中的数据进行操作, 以模拟拍卖行的行为. 而最后发现对数据库直接进行操作经常会导致直接操作的数据和服务端自动管理的数据在 primary key 上存在冲突导致丢失数据. 所以这条路径被窝放弃了.


Price Table
------------------------------------------------------------------------------
我在 Google Sheet 上维护着一个 `cpi-consumer-price-index-data <https://docs.google.com/spreadsheets/d/1e4I2-d4JyVbsvOcdePruqev-rkyYYMUPrwkI_fieIYw/edit?gid=2104698923#gid=2104698923>`_ 表格, 里面记录了常用物品的价格.

这个表最早来自于数据库中的 ``world.item_template`` 表导出的结果, 然后加上了一些 column. 然后我手动填写了一些数据得来的. 我重点填写那些原子的物品.

下面我来介绍一下我是如何维护这个表的.


"编号	名字	单价	可买	大类	小类	品质	绑定	iLvl	rLvl	product_count	reagent_name_1	reagent_count_1	reagent_name_2	reagent_count_2	reagent_name_3	reagent_count_3	reagent_name_4	reagent_count_4	reagent_name_5	reagent_count_5	reagent_name_6	reagent_count_6"

创建 Lookup Table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
这一步我需要将数据库中的 ``item_template`` 表导出到 Google Sheet, 并且删掉明显不会出现在物价表上的物品 (例如灰色物品, 任务物品), 并且将



``item_lookup``

``item_template``


Buy Price Sell Price
------------------------------------------------------------------------------
我会将 Price Table 中的价格设为 Buy Price, 也就是你从 NPC 处购买等效于你跟真人从拍卖行购买. 而 Sell Price 则设为 Buy Price 的 90%.


NPC Vendor
------------------------------------------------------------------------------


后来我发现, 可以直接对 item_template 中的物品的价格进行修改, 将 buy price 设为参考价,
也就是玩家可以用参考价从 NPC 处购买. 而 sell price 则设为参考价的 90%. 然后创建一些 NPC
vendor 在达拉然能卖这些东西即可.