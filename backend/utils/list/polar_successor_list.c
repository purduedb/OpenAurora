/*----------------------------------------------------------------------------------------
 *
 * polar_successor_list.c
 *  This list is used to manage free resources.
 *  The resources are allocated as array items,
 *  get the free item from polar_successor_list_pop
 *  and call polar_successor_list_push when the item is released.
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
 * src/backend/utils/list/polar_successor_list.c
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/polar_successor_list.h"

polar_successor_list *
polar_successor_list_init(void *ptr, int total_items)
{
	int i;
	polar_successor_list *list = (polar_successor_list *)ptr;

	for (i = 0; i < total_items; i++)
		list->items[i] = i + 1;

	list->items[total_items - 1] = POLAR_SUCCESSOR_LIST_NIL;

	list->free_head = 0;

	return list;
}

int
polar_successor_list_pop(polar_successor_list *list)
{
	int free_item = list->free_head;

	if (free_item != POLAR_SUCCESSOR_LIST_NIL)
	{
		list->free_head = list->items[free_item];
		list->items[free_item] = POLAR_SUCCESSOR_LIST_NIL;
	}

	return free_item;
}

void
polar_successor_list_push(polar_successor_list *list, int free_item)
{
	if (list->free_head != POLAR_SUCCESSOR_LIST_NIL)
		list->items[free_item] = list->free_head;
	else
		list->items[free_item] = POLAR_SUCCESSOR_LIST_NIL;

	list->free_head = free_item;
}
