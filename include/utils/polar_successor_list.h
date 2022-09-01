/*----------------------------------------------------------------------------------------
 *
 * polar_successor_list.h
 *  This list is used to manage free resources.
 *  The resources are allocated as array items,
 *  get the free item from polar_successor_list_pop
 *  and call polar_successor_list_push when the item is released.
 *
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * src/include/utils/polar_successor_list.h
 * ---------------------------------------------------------------------------------------
 */

#ifndef POLAR_SUCCESSOR_LIST_H
#define POLAR_SUCCESSOR_LIST_H

#include "pg_config.h"

#define POLAR_SUCCESSOR_LIST_NIL (-1)

typedef struct polar_successor_list
{
	int free_head;
	int items[FLEXIBLE_ARRAY_MEMBER];
} polar_successor_list;

#define POLAR_SUCCESSOR_LIST_SIZE(n) (offsetof(polar_successor_list, items) + (n) * sizeof(int))

#define POLAR_SUCCESSOR_LIST_EMPTY(list) ((list)->free_head == POLAR_SUCCESSOR_LIST_NIL)

/* Init polar_successor_list from allocated memory */
polar_successor_list *polar_successor_list_init(void *ptr, int total_items);
int polar_successor_list_pop(polar_successor_list *list);
void polar_successor_list_push(polar_successor_list *list, int free);

#endif
