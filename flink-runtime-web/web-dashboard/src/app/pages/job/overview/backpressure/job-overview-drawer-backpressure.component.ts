/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, OnDestroy, OnInit, Type } from '@angular/core';
import { of, Subject } from 'rxjs';
import { catchError, mergeMap, takeUntil, tap } from 'rxjs/operators';

import {
  JobBackpressure,
  JobBackpressureSubtask,
  JobBackpressureSubtaskData,
  NodesItemCorrect
} from '@flink-runtime-web/interfaces';
import {
  JOB_OVERVIEW_MODULE_CONFIG,
  JOB_OVERVIEW_MODULE_DEFAULT_CONFIG,
  JobOverviewModuleConfig
} from '@flink-runtime-web/pages/job/overview/job-overview.config';
import { JobService } from '@flink-runtime-web/services';
import { typeDefinition } from '@flink-runtime-web/utils/strong-type';

import { JobLocalService } from '../../job-local.service';

@Component({
  selector: 'flink-job-overview-drawer-backpressure',
  templateUrl: './job-overview-drawer-backpressure.component.html',
  styleUrls: ['./job-overview-drawer-backpressure.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobOverviewDrawerBackpressureComponent implements OnInit, OnDestroy {
  readonly trackBySubtask = (_: number, node: JobBackpressureSubtask): number => node.subtask;
  readonly trackBySubtaskAttempt = (_: number, node: JobBackpressureSubtaskData): string =>
    `${node.subtask}-${node['attempt-number']}`;

  expandSet = new Set<number>();
  isLoading = true;
  now = Date.now();
  selectedVertex: NodesItemCorrect | null;
  backpressure = {} as JobBackpressure;
  listOfSubTaskBackpressure: JobBackpressureSubtask[] = [];
  stateBadgeComponent: Type<unknown>;

  readonly narrowType = typeDefinition<JobBackpressureSubtask>();

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef,
    @Inject(JOB_OVERVIEW_MODULE_CONFIG) readonly moduleConfig: JobOverviewModuleConfig
  ) {
    this.stateBadgeComponent =
      moduleConfig.customComponents?.backpressureBadgeComponent ||
      JOB_OVERVIEW_MODULE_DEFAULT_CONFIG.customComponents.backpressureBadgeComponent;
  }

  ngOnInit(): void {
    this.jobLocalService
      .jobWithVertexChanges()
      .pipe(
        tap(data => {
          this.selectedVertex = data.vertex;
          this.cdr.markForCheck();
        }),
        mergeMap(data =>
          this.jobService.loadOperatorBackPressure(data.job.jid, data.vertex!.id).pipe(
            catchError(() => {
              return of({} as JobBackpressure);
            })
          )
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.isLoading = false;
        this.now = Date.now();
        this.backpressure = data;
        this.listOfSubTaskBackpressure = data?.subtasks || [];
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  collapseAll(): void {
    this.expandSet.clear();
    this.cdr.markForCheck();
  }

  onExpandChange(subtask: JobBackpressureSubtask, checked: boolean): void {
    if (checked) {
      this.expandSet.add(subtask.subtask);
    } else {
      this.expandSet.delete(subtask.subtask);
    }
    this.cdr.markForCheck();
  }

  prettyPrint(value: number): string {
    if (isNaN(value)) {
      return 'N/A';
    } else {
      return `${Math.round(value * 100)}%`;
    }
  }
}
