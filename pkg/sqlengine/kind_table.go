package sqlengine

// Kind identifies the type of an expression node.
type Kind uint16

const (
	KNone                                Kind = 0
	KAIAgg                               Kind = 1
	KAISummarizeAgg                      Kind = 2
	KAnyValue                            Kind = 3
	KApproximateSimilarity               Kind = 4
	KApproxPercentileAccumulate          Kind = 5
	KApproxPercentileCombine             Kind = 6
	KApproxPercentileEstimate            Kind = 7
	KApproxQuantiles                     Kind = 8
	KApproxTopK                          Kind = 9
	KApproxTopKAccumulate                Kind = 10
	KApproxTopKCombine                   Kind = 11
	KApproxTopKEstimate                  Kind = 12
	KApproxTopSum                        Kind = 13
	KArgMax                              Kind = 14
	KArgMin                              Kind = 15
	KArrayAgg                            Kind = 16
	KArrayConcatAgg                      Kind = 17
	KArrayUnionAgg                       Kind = 18
	KArrayUniqueAgg                      Kind = 19
	KAvg                                 Kind = 20
	KCorr                                Kind = 21
	KCount                               Kind = 22
	KCountIf                             Kind = 23
	KCovarPop                            Kind = 24
	KCovarSamp                           Kind = 25
	KCumeDist                            Kind = 26
	KDenseRank                           Kind = 27
	KFirst                               Kind = 28
	KFirstValue                          Kind = 29
	KGroupConcat                         Kind = 30
	KGrouping                            Kind = 31
	KGroupingId                          Kind = 32
	KKurtosis                            Kind = 33
	KLag                                 Kind = 34
	KLast                                Kind = 35
	KLastValue                           Kind = 36
	KLead                                Kind = 37
	KLogicalAnd                          Kind = 38
	KLogicalOr                           Kind = 39
	KMax                                 Kind = 40
	KMedian                              Kind = 41
	KMin                                 Kind = 42
	KMinhash                             Kind = 43
	KMinhashCombine                      Kind = 44
	KMode                                Kind = 45
	KNtile                               Kind = 46
	KNthValue                            Kind = 47
	KObjectAgg                           Kind = 48
	KPercentileCont                      Kind = 49
	KPercentileDisc                      Kind = 50
	KPercentRank                         Kind = 51
	KQuantile                            Kind = 52
	KApproxQuantile                      Kind = 53
	KRank                                Kind = 54
	KRegrAvgx                            Kind = 55
	KRegrAvgy                            Kind = 56
	KRegrCount                           Kind = 57
	KRegrIntercept                       Kind = 58
	KRegrR2                              Kind = 59
	KRegrSlope                           Kind = 60
	KRegrSxx                             Kind = 61
	KRegrSxy                             Kind = 62
	KRegrSyy                             Kind = 63
	KRegrValx                            Kind = 64
	KRegrValy                            Kind = 65
	KRowNumber                           Kind = 66
	KSkewness                            Kind = 67
	KStddev                              Kind = 68
	KStddevPop                           Kind = 69
	KStddevSamp                          Kind = 70
	KSum                                 Kind = 71
	KVariance                            Kind = 72
	KVariancePop                         Kind = 73
	KArray                               Kind = 74
	KArrayConstructCompact               Kind = 75
	KList                                Kind = 76
	KToArray                             Kind = 77
	KArrayAppend                         Kind = 78
	KArrayCompact                        Kind = 79
	KArrayConcat                         Kind = 80
	KArrayFilter                         Kind = 81
	KArrayInsert                         Kind = 82
	KArrayPrepend                        Kind = 83
	KArrayRemove                         Kind = 84
	KArrayRemoveAt                       Kind = 85
	KArrayReverse                        Kind = 86
	KArraySlice                          Kind = 87
	KArraySort                           Kind = 88
	KSortArray                           Kind = 89
	KArrayAll                            Kind = 90
	KArrayAny                            Kind = 91
	KArrayContains                       Kind = 92
	KArrayContainsAll                    Kind = 93
	KArrayContainedBy                    Kind = 94
	KArrayExcept                         Kind = 95
	KArrayIntersect                      Kind = 96
	KArrayOverlaps                       Kind = 97
	KArrayPosition                       Kind = 98
	KArrayDistinct                       Kind = 99
	KArrayFirst                          Kind = 100
	KArrayLast                           Kind = 101
	KArrayMax                            Kind = 102
	KArrayMin                            Kind = 103
	KArraySize                           Kind = 104
	KArraySum                            Kind = 105
	KArraysZip                           Kind = 106
	KArrayToString                       Kind = 107
	KFlatten                             Kind = 108
	KStringToArray                       Kind = 109
	KStrtokToArray                       Kind = 110
	KApply                               Kind = 111
	KReduce                              Kind = 112
	KTransform                           Kind = 113
	KGenerateSeries                      Kind = 114
	KExplodingGenerateSeries             Kind = 115
	KGenerator                           Kind = 116
	KExplode                             Kind = 117
	KInline                              Kind = 118
	KExplodeOuter                        Kind = 119
	K_ExplodeOuter                       Kind = 120
	KPosexplode                          Kind = 121
	KPosexplodeOuter                     Kind = 122
	KPositionalColumn                    Kind = 123
	KUnnest                              Kind = 124
	KMap                                 Kind = 125
	KMapCat                              Kind = 126
	KMapContainsKey                      Kind = 127
	KMapDelete                           Kind = 128
	KMapFromEntries                      Kind = 129
	KMapInsert                           Kind = 130
	KMapKeys                             Kind = 131
	KMapPick                             Kind = 132
	KMapSize                             Kind = 133
	KStarMap                             Kind = 134
	KToMap                               Kind = 135
	KVarMap                              Kind = 136
	KStruct                              Kind = 137
	KStructExtract                       Kind = 138
	KStDistance                          Kind = 139
	KStPoint                             Kind = 140
	KIndexConstraintOption               Kind = 141
	KReference                           Kind = 142
	KColumnConstraint                    Kind = 143
	KAutoIncrementColumnConstraint       Kind = 144
	KInvisibleColumnConstraint           Kind = 145
	KZeroFillColumnConstraint            Kind = 146
	KPeriodForSystemTimeConstraint       Kind = 147
	KCaseSpecificColumnConstraint        Kind = 148
	KCharacterSetColumnConstraint        Kind = 149
	KCheckColumnConstraint               Kind = 150
	KAssumeColumnConstraint              Kind = 151
	KClusteredColumnConstraint           Kind = 152
	KCollateColumnConstraint             Kind = 153
	KCommentColumnConstraint             Kind = 154
	KCompressColumnConstraint            Kind = 155
	KDateFormatColumnConstraint          Kind = 156
	KDefaultColumnConstraint             Kind = 157
	KEncodeColumnConstraint              Kind = 158
	KExcludeColumnConstraint             Kind = 159
	KEphemeralColumnConstraint           Kind = 160
	KWithOperator                        Kind = 161
	KGeneratedAsIdentityColumnConstraint Kind = 162
	KGeneratedAsRowColumnConstraint      Kind = 163
	KIndexColumnConstraint               Kind = 164
	KInlineLengthColumnConstraint        Kind = 165
	KNonClusteredColumnConstraint        Kind = 166
	KNotForReplicationColumnConstraint   Kind = 167
	KMaskingPolicyColumnConstraint       Kind = 168
	KNotNullColumnConstraint             Kind = 169
	KOnUpdateColumnConstraint            Kind = 170
	KPrimaryKeyColumnConstraint          Kind = 171
	KTitleColumnConstraint               Kind = 172
	KUniqueColumnConstraint              Kind = 173
	KUppercaseColumnConstraint           Kind = 174
	KWatermarkColumnConstraint           Kind = 175
	KPathColumnConstraint                Kind = 176
	KProjectionPolicyColumnConstraint    Kind = 177
	KComputedColumnConstraint            Kind = 178
	KInOutColumnConstraint               Kind = 179
	KConstraint                          Kind = 180
	KForeignKey                          Kind = 181
	KColumnPrefix                        Kind = 182
	KPrimaryKey                          Kind = 183
	KTimeseriesKey                       Kind = 184
	KIndexParameters                     Kind = 185
	KAddConstraint                       Kind = 186
	KExpr                                Kind = 187
	KExpression                          Kind = 188
	KCondition                           Kind = 189
	KPredicate                           Kind = 190
	KCache                               Kind = 191
	KUncache                             Kind = 192
	KRefresh                             Kind = 193
	KLockingStatement                    Kind = 194
	KColumnConstraintKind                Kind = 195
	KSubqueryPredicate                   Kind = 196
	KAll                                 Kind = 197
	KAny                                 Kind = 198
	KBinary                              Kind = 199
	KConnector                           Kind = 200
	KFunc                                Kind = 201
	KAggFunc                             Kind = 202
	KColumn                              Kind = 203
	KLiteral                             Kind = 204
	KVar                                 Kind = 205
	KWithinGroup                         Kind = 206
	KPseudocolumn                        Kind = 207
	KHint                                Kind = 208
	KJoinHint                            Kind = 209
	KIdentifier                          Kind = 210
	KDynamicIdentifier                   Kind = 211
	KOpclass                             Kind = 212
	KStar                                Kind = 213
	KParameter                           Kind = 214
	KSessionParameter                    Kind = 215
	KPlaceholder                         Kind = 216
	KNull                                Kind = 217
	KBoolean                             Kind = 218
	KDot                                 Kind = 219
	KKwarg                               Kind = 220
	KAlias                               Kind = 221
	KPivotAlias                          Kind = 222
	KPivotAny                            Kind = 223
	KAliases                             Kind = 224
	KBracket                             Kind = 225
	KForIn                               Kind = 226
	KIgnoreNulls                         Kind = 227
	KRespectNulls                        Kind = 228
	KHavingMax                           Kind = 229
	KSafeFunc                            Kind = 230
	KTypeof                              Kind = 231
	KParameterizedAgg                    Kind = 232
	KAnonymous                           Kind = 233
	KAnonymousAggFunc                    Kind = 234
	KCombinedAggFunc                     Kind = 235
	KCombinedParameterizedAgg            Kind = 236
	KHashAgg                             Kind = 237
	KHll                                 Kind = 238
	KApproxDistinct                      Kind = 239
	KSlice                               Kind = 240
	KTimeUnit                            Kind = 241
	K_TimeUnit                           Kind = 242
	KIntervalOp                          Kind = 243
	KFilter                              Kind = 244
	KCheck                               Kind = 245
	KOrdered                             Kind = 246
	KAdd                                 Kind = 247
	KBitwiseAnd                          Kind = 248
	KBitwiseLeftShift                    Kind = 249
	KBitwiseOr                           Kind = 250
	KBitwiseRightShift                   Kind = 251
	KBitwiseXor                          Kind = 252
	KDiv                                 Kind = 253
	KOverlaps                            Kind = 254
	KExtendsLeft                         Kind = 255
	KExtendsRight                        Kind = 256
	KDPipe                               Kind = 257
	KEQ                                  Kind = 258
	KNullSafeEQ                          Kind = 259
	KNullSafeNEQ                         Kind = 260
	KPropertyEQ                          Kind = 261
	KDistance                            Kind = 262
	KDistanceNd                          Kind = 263
	KEscape                              Kind = 264
	KGlob                                Kind = 265
	KGT                                  Kind = 266
	KGTE                                 Kind = 267
	KILike                               Kind = 268
	KIntDiv                              Kind = 269
	KIs                                  Kind = 270
	KLike                                Kind = 271
	KMatch                               Kind = 272
	KLT                                  Kind = 273
	KLTE                                 Kind = 274
	KMod                                 Kind = 275
	KMul                                 Kind = 276
	KNEQ                                 Kind = 277
	KNestedJSONSelect                    Kind = 278
	KOperator                            Kind = 279
	KSimilarTo                           Kind = 280
	KSub                                 Kind = 281
	KAdjacent                            Kind = 282
	KUnary                               Kind = 283
	KBitwiseNot                          Kind = 284
	KNot                                 Kind = 285
	KParen                               Kind = 286
	KNeg                                 Kind = 287
	KAtIndex                             Kind = 288
	KAtTimeZone                          Kind = 289
	KFromTimeZone                        Kind = 290
	KFormatPhrase                        Kind = 291
	KBetween                             Kind = 292
	KDistinct                            Kind = 293
	KIn                                  Kind = 294
	KAnd                                 Kind = 295
	KOr                                  Kind = 296
	KXor                                 Kind = 297
	KPow                                 Kind = 298
	KRegexpLike                          Kind = 299
	KDataTypeParam                       Kind = 300
	KDataType                            Kind = 301
	KPseudoType                          Kind = 302
	KObjectIdentifier                    Kind = 303
	KIntervalSpan                        Kind = 304
	KInterval                            Kind = 305
	KDDL                                 Kind = 306
	KCreate                              Kind = 307
	KSequenceProperties                  Kind = 308
	KTriggerProperties                   Kind = 309
	KTriggerExecute                      Kind = 310
	KTriggerEvent                        Kind = 311
	KTriggerReferencing                  Kind = 312
	KTruncateTable                       Kind = 313
	KClone                               Kind = 314
	KDescribe                            Kind = 315
	KAttach                              Kind = 316
	KDetach                              Kind = 317
	KInstall                             Kind = 318
	KSummarize                           Kind = 319
	KKill                                Kind = 320
	KPragma                              Kind = 321
	KDeclare                             Kind = 322
	KDeclareItem                         Kind = 323
	KSet                                 Kind = 324
	KHeredoc                             Kind = 325
	KSetItem                             Kind = 326
	KShow                                Kind = 327
	KUserDefinedFunction                 Kind = 328
	KMacroOverloads                      Kind = 329
	KMacroOverload                       Kind = 330
	KCharacterSet                        Kind = 331
	KAlterColumn                         Kind = 332
	KModifyColumn                        Kind = 333
	KAlterIndex                          Kind = 334
	KAlterDistStyle                      Kind = 335
	KAlterSortKey                        Kind = 336
	KAlterSet                            Kind = 337
	KRenameColumn                        Kind = 338
	KAlterRename                         Kind = 339
	KRenameIndex                         Kind = 340
	KAlterModifySqlSecurity              Kind = 341
	KSwapTable                           Kind = 342
	KComment                             Kind = 343
	KComprehension                       Kind = 344
	KMergeTreeTTLAction                  Kind = 345
	KMergeTreeTTL                        Kind = 346
	KDrop                                Kind = 347
	KDropPrimaryKey                      Kind = 348
	KUndrop                              Kind = 349
	KCommand                             Kind = 350
	KTransaction                         Kind = 351
	KCommit                              Kind = 352
	KRollback                            Kind = 353
	KAlter                               Kind = 354
	KAlterSession                        Kind = 355
	KUse                                 Kind = 356
	KNextValueFor                        Kind = 357
	KExecute                             Kind = 358
	KExecuteSql                          Kind = 359
	KDML                                 Kind = 360
	KDelete                              Kind = 361
	KExport                              Kind = 362
	KCopyParameter                       Kind = 363
	KCopy                                Kind = 364
	KCredentials                         Kind = 365
	KDirectory                           Kind = 366
	KDirectoryStage                      Kind = 367
	KInsert                              Kind = 368
	KOnConflict                          Kind = 369
	KReturning                           Kind = 370
	KLoadData                            Kind = 371
	KUpdate                              Kind = 372
	KMerge                               Kind = 373
	KWhen                                Kind = 374
	KWhens                               Kind = 375
	KCast                                Kind = 376
	KTryCast                             Kind = 377
	KJSONCast                            Kind = 378
	KCastToStrType                       Kind = 379
	KConvert                             Kind = 380
	KIf                                  Kind = 381
	KCase                                Kind = 382
	KCoalesce                            Kind = 383
	KDecodeCase                          Kind = 384
	KEqualNull                           Kind = 385
	KGreatest                            Kind = 386
	KLeast                               Kind = 387
	KNullif                              Kind = 388
	KObjectTransform                     Kind = 389
	KNvl2                                Kind = 390
	KTry                                 Kind = 391
	KCollate                             Kind = 392
	KCollation                           Kind = 393
	KConnectByRoot                       Kind = 394
	KCheckXml                            Kind = 395
	KExists                              Kind = 396
	KFloat64                             Kind = 397
	KInt64                               Kind = 398
	KIsArray                             Kind = 399
	KIsNullValue                         Kind = 400
	KLaxBool                             Kind = 401
	KLaxFloat64                          Kind = 402
	KLaxInt64                            Kind = 403
	KLaxString                           Kind = 404
	KToBoolean                           Kind = 405
	KToVariant                           Kind = 406
	KCurrentAccount                      Kind = 407
	KCurrentAccountName                  Kind = 408
	KCurrentAvailableRoles               Kind = 409
	KCurrentCatalog                      Kind = 410
	KCurrentClient                       Kind = 411
	KCurrentDatabase                     Kind = 412
	KCurrentIpAddress                    Kind = 413
	KCurrentOrganizationName             Kind = 414
	KCurrentOrganizationUser             Kind = 415
	KCurrentRegion                       Kind = 416
	KCurrentRole                         Kind = 417
	KCurrentRoleType                     Kind = 418
	KCurrentSchema                       Kind = 419
	KCurrentSchemas                      Kind = 420
	KCurrentSecondaryRoles               Kind = 421
	KCurrentSession                      Kind = 422
	KCurrentStatement                    Kind = 423
	KCurrentTransaction                  Kind = 424
	KCurrentUser                         Kind = 425
	KCurrentUserId                       Kind = 426
	KCurrentVersion                      Kind = 427
	KCurrentWarehouse                    Kind = 428
	KSessionUser                         Kind = 429
	KAIClassify                          Kind = 430
	KAIEmbed                             Kind = 431
	KAISimilarity                        Kind = 432
	KAIGenerate                          Kind = 433
	KFeaturesAtTime                      Kind = 434
	KGenerateEmbedding                   Kind = 435
	KGenerateText                        Kind = 436
	KGenerateTable                       Kind = 437
	KGenerateBool                        Kind = 438
	KGenerateInt                         Kind = 439
	KGenerateDouble                      Kind = 440
	KMLForecast                          Kind = 441
	KAIForecast                          Kind = 442
	KMLTranslate                         Kind = 443
	KPredict                             Kind = 444
	KVectorSearch                        Kind = 445
	KReadCSV                             Kind = 446
	KReadParquet                         Kind = 447
	KXMLElement                          Kind = 448
	KGetIgnoreCase                       Kind = 449
	KXMLGet                              Kind = 450
	KXMLTable                            Kind = 451
	KHost                                Kind = 452
	KNetFunc                             Kind = 453
	KParseIp                             Kind = 454
	KRegDomain                           Kind = 455
	KColumns                             Kind = 456
	KNormal                              Kind = 457
	KRand                                Kind = 458
	KRandn                               Kind = 459
	KRandstr                             Kind = 460
	KRangeBucket                         Kind = 461
	KRangeN                              Kind = 462
	KSeq1                                Kind = 463
	KSeq2                                Kind = 464
	KSeq4                                Kind = 465
	KSeq8                                Kind = 466
	KUniform                             Kind = 467
	KUuid                                Kind = 468
	KWeekStart                           Kind = 469
	KWidthBucket                         Kind = 470
	KZipf                                Kind = 471
	KCheckJson                           Kind = 472
	KJSONArray                           Kind = 473
	KJSONArrayAgg                        Kind = 474
	KJSONArrayAppend                     Kind = 475
	KJSONArrayContains                   Kind = 476
	KJSONArrayInsert                     Kind = 477
	KJSONBContains                       Kind = 478
	KJSONBContainsAllTopKeys             Kind = 479
	KJSONBContainsAnyTopKeys             Kind = 480
	KJSONBDeleteAtPath                   Kind = 481
	KJSONBPathExists                     Kind = 482
	KJSONBExists                         Kind = 483
	KJSONBExtract                        Kind = 484
	KJSONBExtractScalar                  Kind = 485
	KJSONBObjectAgg                      Kind = 486
	KJSONBool                            Kind = 487
	KJSONExists                          Kind = 488
	KJSONExtract                         Kind = 489
	KJSONExtractArray                    Kind = 490
	KJSONExtractScalar                   Kind = 491
	KJSONFormat                          Kind = 492
	KJSONKeys                            Kind = 493
	KJSONKeysAtDepth                     Kind = 494
	KJSONObject                          Kind = 495
	KJSONObjectAgg                       Kind = 496
	KJSONRemove                          Kind = 497
	KJSONSet                             Kind = 498
	KJSONStripNulls                      Kind = 499
	KStripNullValue                      Kind = 500
	KJSONTable                           Kind = 501
	KJSONType                            Kind = 502
	KObjectId                            Kind = 503
	KObjectInsert                        Kind = 504
	KOpenJSON                            Kind = 505
	KParseJSON                           Kind = 506
	KAcos                                Kind = 507
	KAcosh                               Kind = 508
	KAsin                                Kind = 509
	KAsinh                               Kind = 510
	KAtan                                Kind = 511
	KAtanh                               Kind = 512
	KAtan2                               Kind = 513
	KCos                                 Kind = 514
	KCosh                                Kind = 515
	KCot                                 Kind = 516
	KCoth                                Kind = 517
	KCsc                                 Kind = 518
	KCsch                                Kind = 519
	KDegrees                             Kind = 520
	KRadians                             Kind = 521
	KSec                                 Kind = 522
	KSech                                Kind = 523
	KSin                                 Kind = 524
	KSinh                                Kind = 525
	KTan                                 Kind = 526
	KTanh                                Kind = 527
	KCosineDistance                      Kind = 528
	KDotProduct                          Kind = 529
	KEuclideanDistance                   Kind = 530
	KJarowinklerSimilarity               Kind = 531
	KManhattanDistance                   Kind = 532
	KAbs                                 Kind = 533
	KCbrt                                Kind = 534
	KCeil                                Kind = 535
	KExp                                 Kind = 536
	KFactorial                           Kind = 537
	KFloor                               Kind = 538
	KIsInf                               Kind = 539
	KIsNan                               Kind = 540
	KLn                                  Kind = 541
	KLog                                 Kind = 542
	KPi                                  Kind = 543
	KRint                                Kind = 544
	KRound                               Kind = 545
	KSign                                Kind = 546
	KSqrt                                Kind = 547
	KTrunc                               Kind = 548
	KSafeAdd                             Kind = 549
	KSafeDivide                          Kind = 550
	KSafeMultiply                        Kind = 551
	KSafeNegate                          Kind = 552
	KSafeSubtract                        Kind = 553
	KBitwiseAndAgg                       Kind = 554
	KBitwiseCount                        Kind = 555
	KBitwiseOrAgg                        Kind = 556
	KBitwiseXorAgg                       Kind = 557
	KBitmapBitPosition                   Kind = 558
	KBitmapBucketNumber                  Kind = 559
	KBitmapConstructAgg                  Kind = 560
	KBitmapCount                         Kind = 561
	KBitmapOrAgg                         Kind = 562
	KBooland                             Kind = 563
	KBoolnot                             Kind = 564
	KBoolor                              Kind = 565
	KBoolxorAgg                          Kind = 566
	KGetbit                              Kind = 567
	KProperty                            Kind = 568
	KGrantPrivilege                      Kind = 569
	KGrantPrincipal                      Kind = 570
	KAllowedValuesProperty               Kind = 571
	KAlgorithmProperty                   Kind = 572
	KApiProperty                         Kind = 573
	KApplicationProperty                 Kind = 574
	KAutoIncrementProperty               Kind = 575
	KAutoRefreshProperty                 Kind = 576
	KBackupProperty                      Kind = 577
	KBuildProperty                       Kind = 578
	KBlockCompressionProperty            Kind = 579
	KCalledOnNullInputProperty           Kind = 580
	KCatalogProperty                     Kind = 581
	KCharacterSetProperty                Kind = 582
	KChecksumProperty                    Kind = 583
	KClusterProperty                     Kind = 584
	KCollateProperty                     Kind = 585
	KComputeProperty                     Kind = 586
	KCopyGrantsProperty                  Kind = 587
	KDataBlocksizeProperty               Kind = 588
	KDataDeletionProperty                Kind = 589
	KDatabaseProperty                    Kind = 590
	KDefinerProperty                     Kind = 591
	KDistKeyProperty                     Kind = 592
	KDistributedByProperty               Kind = 593
	KDistStyleProperty                   Kind = 594
	KDuplicateKeyProperty                Kind = 595
	KEngineProperty                      Kind = 596
	KUuidProperty                        Kind = 597
	KHeapProperty                        Kind = 598
	KHybridProperty                      Kind = 599
	KHandlerProperty                     Kind = 600
	KParameterStyleProperty              Kind = 601
	KToTableProperty                     Kind = 602
	KExecuteAsProperty                   Kind = 603
	KExternalProperty                    Kind = 604
	KFallbackProperty                    Kind = 605
	KFileFormatProperty                  Kind = 606
	KCredentialsProperty                 Kind = 607
	KFreespaceProperty                   Kind = 608
	KGlobalProperty                      Kind = 609
	KIcebergProperty                     Kind = 610
	KInheritsProperty                    Kind = 611
	KInputModelProperty                  Kind = 612
	KOutputModelProperty                 Kind = 613
	KIsolatedLoadingProperty             Kind = 614
	KJournalProperty                     Kind = 615
	KLanguageProperty                    Kind = 616
	KEnviromentProperty                  Kind = 617
	KClusteredByProperty                 Kind = 618
	KDictProperty                        Kind = 619
	KDictSubProperty                     Kind = 620
	KDictRange                           Kind = 621
	KDynamicProperty                     Kind = 622
	KOnCluster                           Kind = 623
	KEmptyProperty                       Kind = 624
	KLikeProperty                        Kind = 625
	KLocationProperty                    Kind = 626
	KLockProperty                        Kind = 627
	KLockingProperty                     Kind = 628
	KLogProperty                         Kind = 629
	KMaskingProperty                     Kind = 630
	KMaterializedProperty                Kind = 631
	KMergeBlockRatioProperty             Kind = 632
	KModuleProperty                      Kind = 633
	KNetworkProperty                     Kind = 634
	KNoPrimaryIndexProperty              Kind = 635
	KOnProperty                          Kind = 636
	KOnCommitProperty                    Kind = 637
	KPartitionedByProperty               Kind = 638
	KPartitionedByBucket                 Kind = 639
	KPartitionByTruncate                 Kind = 640
	KPartitionByRangeProperty            Kind = 641
	KPartitionByRangePropertyDynamic     Kind = 642
	KRollupProperty                      Kind = 643
	KRollupIndex                         Kind = 644
	KRowAccessProperty                   Kind = 645
	KPartitionByListProperty             Kind = 646
	KPartitionList                       Kind = 647
	KRefreshTriggerProperty              Kind = 648
	KUniqueKeyProperty                   Kind = 649
	KPartitionBoundSpec                  Kind = 650
	KPartitionedOfProperty               Kind = 651
	KStreamingTableProperty              Kind = 652
	KRemoteWithConnectionModelProperty   Kind = 653
	KReturnsProperty                     Kind = 654
	KStrictProperty                      Kind = 655
	KRowFormatProperty                   Kind = 656
	KRowFormatDelimitedProperty          Kind = 657
	KRowFormatSerdeProperty              Kind = 658
	KQueryTransform                      Kind = 659
	KSampleProperty                      Kind = 660
	KSchemaCommentProperty               Kind = 661
	KSemanticView                        Kind = 662
	KSerdeProperties                     Kind = 663
	KSetProperty                         Kind = 664
	KSharingProperty                     Kind = 665
	KSetConfigProperty                   Kind = 666
	KSettingsProperty                    Kind = 667
	KSortKeyProperty                     Kind = 668
	KSqlReadWriteProperty                Kind = 669
	KSqlSecurityProperty                 Kind = 670
	KStabilityProperty                   Kind = 671
	KStorageHandlerProperty              Kind = 672
	KUsingProperty                       Kind = 673
	KTemporaryProperty                   Kind = 674
	KVirtualProperty                     Kind = 675
	KSecureProperty                      Kind = 676
	KSecurityIntegrationProperty         Kind = 677
	KTags                                Kind = 678
	KTransformModelProperty              Kind = 679
	KTransientProperty                   Kind = 680
	KUnloggedProperty                    Kind = 681
	KUsingTemplateProperty               Kind = 682
	KViewAttributeProperty               Kind = 683
	KVolatileProperty                    Kind = 684
	KWithDataProperty                    Kind = 685
	KWithJournalTableProperty            Kind = 686
	KWithSchemaBindingProperty           Kind = 687
	KWithSystemVersioningProperty        Kind = 688
	KWithProcedureOptions                Kind = 689
	KEncodeProperty                      Kind = 690
	KIncludeProperty                     Kind = 691
	KForceProperty                       Kind = 692
	KProperties                          Kind = 693
	KSelectable                          Kind = 694
	KDerivedTable                        Kind = 695
	KUDTF                                Kind = 696
	KQuery                               Kind = 697
	KQueryBand                           Kind = 698
	KRecursiveWithSearch                 Kind = 699
	KWith                                Kind = 700
	KCTE                                 Kind = 701
	KProjectionDef                       Kind = 702
	KTableAlias                          Kind = 703
	KBitString                           Kind = 704
	KHexString                           Kind = 705
	KByteString                          Kind = 706
	KRawString                           Kind = 707
	KUnicodeString                       Kind = 708
	KColumnPosition                      Kind = 709
	KColumnDef                           Kind = 710
	KChanges                             Kind = 711
	KConnect                             Kind = 712
	KPrior                               Kind = 713
	KInto                                Kind = 714
	KFrom                                Kind = 715
	KHaving                              Kind = 716
	KIndex                               Kind = 717
	KConditionalInsert                   Kind = 718
	KMultitableInserts                   Kind = 719
	KOnCondition                         Kind = 720
	KIntroducer                          Kind = 721
	KNational                            Kind = 722
	KPartition                           Kind = 723
	KPartitionRange                      Kind = 724
	KPartitionId                         Kind = 725
	KFetch                               Kind = 726
	KGrant                               Kind = 727
	KRevoke                              Kind = 728
	KGroup                               Kind = 729
	KCube                                Kind = 730
	KRollup                              Kind = 731
	KGroupingSets                        Kind = 732
	KLambda                              Kind = 733
	KLimit                               Kind = 734
	KLimitOptions                        Kind = 735
	KJoin                                Kind = 736
	KLateral                             Kind = 737
	KTableFromRows                       Kind = 738
	KMatchRecognizeMeasure               Kind = 739
	KMatchRecognize                      Kind = 740
	KFinal                               Kind = 741
	KOffset                              Kind = 742
	KOrder                               Kind = 743
	KWithFill                            Kind = 744
	KSkipJSONColumn                      Kind = 745
	KCluster                             Kind = 746
	KDistribute                          Kind = 747
	KSort                                Kind = 748
	KQualify                             Kind = 749
	KInputOutputFormat                   Kind = 750
	KReturn                              Kind = 751
	KTuple                               Kind = 752
	KQueryOption                         Kind = 753
	KForClause                           Kind = 754
	KWithTableHint                       Kind = 755
	KIndexTableHint                      Kind = 756
	KHistoricalData                      Kind = 757
	KPut                                 Kind = 758
	KGet                                 Kind = 759
	KTable                               Kind = 760
	KSetOperation                        Kind = 761
	KUnion                               Kind = 762
	KExcept                              Kind = 763
	KIntersect                           Kind = 764
	KValues                              Kind = 765
	KVersion                             Kind = 766
	KSchema                              Kind = 767
	KLock                                Kind = 768
	KSelect                              Kind = 769
	KSubquery                            Kind = 770
	KTableSample                         Kind = 771
	KTag                                 Kind = 772
	KPivot                               Kind = 773
	KUnpivotColumns                      Kind = 774
	KWindow                              Kind = 775
	KWindowSpec                          Kind = 776
	KPreWhere                            Kind = 777
	KWhere                               Kind = 778
	KAnalyze                             Kind = 779
	KAnalyzeStatistics                   Kind = 780
	KAnalyzeHistogram                    Kind = 781
	KAnalyzeSample                       Kind = 782
	KAnalyzeListChainedRows              Kind = 783
	KAnalyzeDelete                       Kind = 784
	KAnalyzeWith                         Kind = 785
	KAnalyzeValidate                     Kind = 786
	KAnalyzeColumns                      Kind = 787
	KUsingData                           Kind = 788
	KAddPartition                        Kind = 789
	KAttachOption                        Kind = 790
	KDropPartition                       Kind = 791
	KReplacePartition                    Kind = 792
	KTranslateCharacters                 Kind = 793
	KOverflowTruncateBehavior            Kind = 794
	KJSON                                Kind = 795
	KJSONPath                            Kind = 796
	KJSONPathPart                        Kind = 797
	KJSONPathFilter                      Kind = 798
	KJSONPathKey                         Kind = 799
	KJSONPathRecursive                   Kind = 800
	KJSONPathRoot                        Kind = 801
	KJSONPathScript                      Kind = 802
	KJSONPathSlice                       Kind = 803
	KJSONPathSelector                    Kind = 804
	KJSONPathSubscript                   Kind = 805
	KJSONPathUnion                       Kind = 806
	KJSONPathWildcard                    Kind = 807
	KFormatJson                          Kind = 808
	KJSONKeyValue                        Kind = 809
	KJSONColumnDef                       Kind = 810
	KJSONSchema                          Kind = 811
	KJSONValue                           Kind = 812
	KJSONValueArray                      Kind = 813
	KOpenJSONColumnDef                   Kind = 814
	KJSONExtractQuote                    Kind = 815
	KScopeResolution                     Kind = 816
	KStream                              Kind = 817
	KModelAttribute                      Kind = 818
	KXMLNamespace                        Kind = 819
	KXMLKeyValueOption                   Kind = 820
	KSemicolon                           Kind = 821
	KTableColumn                         Kind = 822
	KVariadic                            Kind = 823
	KStoredProcedure                     Kind = 824
	KBlock                               Kind = 825
	KIfBlock                             Kind = 826
	KWhileBlock                          Kind = 827
	KEndStatement                        Kind = 828
	KAscii                               Kind = 829
	KBitLength                           Kind = 830
	KByteLength                          Kind = 831
	KChr                                 Kind = 832
	KConcat                              Kind = 833
	KConcatWs                            Kind = 834
	KContains                            Kind = 835
	KElt                                 Kind = 836
	KEndsWith                            Kind = 837
	KFormat                              Kind = 838
	KInitcap                             Kind = 839
	KIsAscii                             Kind = 840
	KLeft                                Kind = 841
	KLength                              Kind = 842
	KLevenshtein                         Kind = 843
	KLower                               Kind = 844
	KMatchAgainst                        Kind = 845
	KNormalize                           Kind = 846
	KNumberToStr                         Kind = 847
	KOverlay                             Kind = 848
	KPad                                 Kind = 849
	KRepeat                              Kind = 850
	KReplace                             Kind = 851
	KReverse                             Kind = 852
	KRight                               Kind = 853
	KRtrimmedLength                      Kind = 854
	KSearch                              Kind = 855
	KSearchIp                            Kind = 856
	KSoundex                             Kind = 857
	KSoundexP123                         Kind = 858
	KSpace                               Kind = 859
	KSplit                               Kind = 860
	KSplitPart                           Kind = 861
	KStrtok                              Kind = 862
	KStartsWith                          Kind = 863
	KStrPosition                         Kind = 864
	KStrToMap                            Kind = 865
	KString                              Kind = 866
	KStuff                               Kind = 867
	KSubstring                           Kind = 868
	KSubstringIndex                      Kind = 869
	KTranslate                           Kind = 870
	KTrim                                Kind = 871
	KUnicode                             Kind = 872
	KUpper                               Kind = 873
	KBase64DecodeBinary                  Kind = 874
	KBase64DecodeString                  Kind = 875
	KBase64Encode                        Kind = 876
	KCodePointsToBytes                   Kind = 877
	KCodePointsToString                  Kind = 878
	KConvertToCharset                    Kind = 879
	KDecode                              Kind = 880
	KEncode                              Kind = 881
	KFromBase                            Kind = 882
	KFromBase32                          Kind = 883
	KFromBase64                          Kind = 884
	KHex                                 Kind = 885
	KHexDecodeString                     Kind = 886
	KLowerHex                            Kind = 887
	KSafeConvertBytesToString            Kind = 888
	KToBase32                            Kind = 889
	KToBase64                            Kind = 890
	KToBinary                            Kind = 891
	KToChar                              Kind = 892
	KToCodePoints                        Kind = 893
	KToDecfloat                          Kind = 894
	KToDouble                            Kind = 895
	KToFile                              Kind = 896
	KToNumber                            Kind = 897
	KTryBase64DecodeBinary               Kind = 898
	KTryBase64DecodeString               Kind = 899
	KTryHexDecodeBinary                  Kind = 900
	KTryHexDecodeString                  Kind = 901
	KTryToDecfloat                       Kind = 902
	KUnhex                               Kind = 903
	KRegexpCount                         Kind = 904
	KRegexpExtract                       Kind = 905
	KRegexpExtractAll                    Kind = 906
	KRegexpFullMatch                     Kind = 907
	KRegexpILike                         Kind = 908
	KRegexpInstr                         Kind = 909
	KRegexpReplace                       Kind = 910
	KRegexpSplit                         Kind = 911
	KRegexpSubstr                        Kind = 912
	KCompress                            Kind = 913
	KDecrypt                             Kind = 914
	KDecryptRaw                          Kind = 915
	KDecompressBinary                    Kind = 916
	KDecompressString                    Kind = 917
	KEncrypt                             Kind = 918
	KEncryptRaw                          Kind = 919
	KCityHash64                          Kind = 920
	KFarmFingerprint                     Kind = 921
	KMD5                                 Kind = 922
	KMD5Digest                           Kind = 923
	KMD5NumberLower64                    Kind = 924
	KMD5NumberUpper64                    Kind = 925
	KSHA                                 Kind = 926
	KSHA1Digest                          Kind = 927
	KSHA2                                Kind = 928
	KSHA2Digest                          Kind = 929
	KStandardHash                        Kind = 930
	KParseBignumeric                     Kind = 931
	KParseNumeric                        Kind = 932
	KParseUrl                            Kind = 933
	KCurrentDate                         Kind = 934
	KCurrentDatetime                     Kind = 935
	KCurrentTime                         Kind = 936
	KCurrentTimestamp                    Kind = 937
	KCurrentTimestampLTZ                 Kind = 938
	KCurrentTimezone                     Kind = 939
	KLocaltime                           Kind = 940
	KLocaltimestamp                      Kind = 941
	KSystimestamp                        Kind = 942
	KUtcDate                             Kind = 943
	KUtcTime                             Kind = 944
	KUtcTimestamp                        Kind = 945
	KAddMonths                           Kind = 946
	KDateAdd                             Kind = 947
	KDateBin                             Kind = 948
	KDateDiff                            Kind = 949
	KDateSub                             Kind = 950
	KDatetimeAdd                         Kind = 951
	KDatetimeDiff                        Kind = 952
	KDatetimeSub                         Kind = 953
	KMonthsBetween                       Kind = 954
	KTimeAdd                             Kind = 955
	KTimeDiff                            Kind = 956
	KTimeSub                             Kind = 957
	KTimestampAdd                        Kind = 958
	KTimestampDiff                       Kind = 959
	KTimestampSub                        Kind = 960
	KTsOrDsAdd                           Kind = 961
	KTsOrDsDiff                          Kind = 962
	KDatetimeTrunc                       Kind = 963
	KDateTrunc                           Kind = 964
	KTimestampTrunc                      Kind = 965
	KTimeSlice                           Kind = 966
	KTimeTrunc                           Kind = 967
	KDay                                 Kind = 968
	KDayOfMonth                          Kind = 969
	KDayOfWeek                           Kind = 970
	KDayOfWeekIso                        Kind = 971
	KDayOfYear                           Kind = 972
	KDayname                             Kind = 973
	KExtract                             Kind = 974
	KGetExtract                          Kind = 975
	KHour                                Kind = 976
	KMinute                              Kind = 977
	KMonth                               Kind = 978
	KMonthname                           Kind = 979
	KQuarter                             Kind = 980
	KSecond                              Kind = 981
	KToDays                              Kind = 982
	KWeek                                Kind = 983
	KWeekOfYear                          Kind = 984
	KYear                                Kind = 985
	KYearOfWeek                          Kind = 986
	KYearOfWeekIso                       Kind = 987
	KDate                                Kind = 988
	KDateFromParts                       Kind = 989
	KDateFromUnixDate                    Kind = 990
	KDatetime                            Kind = 991
	KGapFill                             Kind = 992
	KGenerateDateArray                   Kind = 993
	KGenerateTimestampArray              Kind = 994
	KJustifyDays                         Kind = 995
	KJustifyHours                        Kind = 996
	KJustifyInterval                     Kind = 997
	KLastDay                             Kind = 998
	KMakeInterval                        Kind = 999
	KNextDay                             Kind = 1000
	KPreviousDay                         Kind = 1001
	KTime                                Kind = 1002
	KTimeFromParts                       Kind = 1003
	KTimestamp                           Kind = 1004
	KTimestampFromParts                  Kind = 1005
	KTimestampLtzFromParts               Kind = 1006
	KTimestampTzFromParts                Kind = 1007
	KConvertTimezone                     Kind = 1008
	KDateStrToDate                       Kind = 1009
	KDateToDateStr                       Kind = 1010
	KDateToDi                            Kind = 1011
	KDiToDate                            Kind = 1012
	KFromISO8601Timestamp                Kind = 1013
	KFromISO8601Date                     Kind = 1014
	KFromISO8601TimestampNanos           Kind = 1015
	KParseDatetime                       Kind = 1016
	KParseTime                           Kind = 1017
	KStrToDate                           Kind = 1018
	KStrToTime                           Kind = 1019
	KStrToUnix                           Kind = 1020
	KTimeStrToDate                       Kind = 1021
	KTimeStrToTime                       Kind = 1022
	KTimeStrToUnix                       Kind = 1023
	KTimeToStr                           Kind = 1024
	KTimeToTimeStr                       Kind = 1025
	KTimeToUnix                          Kind = 1026
	KTsOrDiToDi                          Kind = 1027
	KTsOrDsToDate                        Kind = 1028
	KTsOrDsToDateStr                     Kind = 1029
	KTsOrDsToDatetime                    Kind = 1030
	KTsOrDsToTime                        Kind = 1031
	KTsOrDsToTimestamp                   Kind = 1032
	KUnixDate                            Kind = 1033
	KUnixMicros                          Kind = 1034
	KUnixMillis                          Kind = 1035
	KUnixSeconds                         Kind = 1036
	KUnixToStr                           Kind = 1037
	KUnixToTime                          Kind = 1038
	KUnixToTimeStr                       Kind = 1039
	KNearest                             Kind = 1040
	KConnectTo                           Kind = 1041
	KDisconnect                          Kind = 1042
	KPos                                 Kind = 1043
	KFunctionSpecification               Kind = 1044
	KJSONBContainsTopKey                 Kind = 1045
	KPartitionByExcluding                Kind = 1046
	KOptimize                            Kind = 1047
	KTableSpaceProperty                  Kind = 1048
	KPer                                 Kind = 1049
	KPhysicalProperty                    Kind = 1050
	KStorageProperty                     Kind = 1051
	KListStage                           Kind = 1052
	KBinaryColumnConstraint              Kind = 1053
	KWeightString                        Kind = 1054
	KCopyTagsProperty                    Kind = 1055
	KReplaceColumns                      Kind = 1056
	KWithoutRowidProperty                Kind = 1057
	KAggregateKeyProperty                Kind = 1058
	KCaseStatement                       Kind = 1059
	KLoopBlock                           Kind = 1060
	KRepeatBlock                         Kind = 1061
	KLeave                               Kind = 1062
	KIterate                             Kind = 1063
	KOptionsProperty                     Kind = 1064
	KSplitToTable                        Kind = 1065
)

const numKinds = 1066

var kindInfos = [numKinds]kindInfo{
	KAIAgg:                               {name: "AIAgg", key: "aiagg", mro: []Kind{KAIAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"AI_AGG"}},
	KAISummarizeAgg:                      {name: "AISummarizeAgg", key: "aisummarizeagg", mro: []Kind{KAISummarizeAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"AI_SUMMARIZE_AGG"}},
	KAnyValue:                            {name: "AnyValue", key: "anyvalue", mro: []Kind{KAnyValue, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ANY_VALUE"}},
	KApproximateSimilarity:               {name: "ApproximateSimilarity", key: "approximatesimilarity", mro: []Kind{KApproximateSimilarity, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"APPROXIMATE_SIMILARITY", "APPROXIMATE_JACCARD_INDEX"}},
	KApproxPercentileAccumulate:          {name: "ApproxPercentileAccumulate", key: "approxpercentileaccumulate", mro: []Kind{KApproxPercentileAccumulate, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"APPROX_PERCENTILE_ACCUMULATE"}},
	KApproxPercentileCombine:             {name: "ApproxPercentileCombine", key: "approxpercentilecombine", mro: []Kind{KApproxPercentileCombine, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"APPROX_PERCENTILE_COMBINE"}},
	KApproxPercentileEstimate:            {name: "ApproxPercentileEstimate", key: "approxpercentileestimate", mro: []Kind{KApproxPercentileEstimate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"percentile", true}}, flags: 0, sqlNames: []string{"APPROX_PERCENTILE_ESTIMATE"}},
	KApproxQuantiles:                     {name: "ApproxQuantiles", key: "approxquantiles", mro: []Kind{KApproxQuantiles, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"APPROX_QUANTILES"}},
	KApproxTopK:                          {name: "ApproxTopK", key: "approxtopk", mro: []Kind{KApproxTopK, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"counters", false}}, flags: 0, sqlNames: []string{"APPROX_TOP_K"}},
	KApproxTopKAccumulate:                {name: "ApproxTopKAccumulate", key: "approxtopkaccumulate", mro: []Kind{KApproxTopKAccumulate, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"APPROX_TOP_K_ACCUMULATE"}},
	KApproxTopKCombine:                   {name: "ApproxTopKCombine", key: "approxtopkcombine", mro: []Kind{KApproxTopKCombine, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"APPROX_TOP_K_COMBINE"}},
	KApproxTopKEstimate:                  {name: "ApproxTopKEstimate", key: "approxtopkestimate", mro: []Kind{KApproxTopKEstimate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"APPROX_TOP_K_ESTIMATE"}},
	KApproxTopSum:                        {name: "ApproxTopSum", key: "approxtopsum", mro: []Kind{KApproxTopSum, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"count", true}}, flags: 0, sqlNames: []string{"APPROX_TOP_SUM"}},
	KArgMax:                              {name: "ArgMax", key: "argmax", mro: []Kind{KArgMax, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"count", false}}, flags: 0, sqlNames: []string{"ARG_MAX", "ARGMAX", "MAX_BY"}},
	KArgMin:                              {name: "ArgMin", key: "argmin", mro: []Kind{KArgMin, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"count", false}}, flags: 0, sqlNames: []string{"ARG_MIN", "ARGMIN", "MIN_BY"}},
	KArrayAgg:                            {name: "ArrayAgg", key: "arrayagg", mro: []Kind{KArrayAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"nulls_excluded", false}}, flags: 0, sqlNames: []string{"ARRAY_AGG"}},
	KArrayConcatAgg:                      {name: "ArrayConcatAgg", key: "arrayconcatagg", mro: []Kind{KArrayConcatAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ARRAY_CONCAT_AGG"}},
	KArrayUnionAgg:                       {name: "ArrayUnionAgg", key: "arrayunionagg", mro: []Kind{KArrayUnionAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ARRAY_UNION_AGG"}},
	KArrayUniqueAgg:                      {name: "ArrayUniqueAgg", key: "arrayuniqueagg", mro: []Kind{KArrayUniqueAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ARRAY_UNIQUE_AGG"}},
	KAvg:                                 {name: "Avg", key: "avg", mro: []Kind{KAvg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"AVG"}},
	KCorr:                                {name: "Corr", key: "corr", mro: []Kind{KCorr, KExpression, KAggFunc, KFunc, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"null_on_zero_variance", false}}, flags: 0, sqlNames: []string{"CORR"}},
	KCount:                               {name: "Count", key: "count", mro: []Kind{KCount, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}, {"big_int", false}}, flags: kfVarLenArgs, sqlNames: []string{"COUNT"}},
	KCountIf:                             {name: "CountIf", key: "countif", mro: []Kind{KCountIf, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"zero_on_all_null", false}}, flags: 0, sqlNames: []string{"COUNT_IF", "COUNTIF"}},
	KCovarPop:                            {name: "CovarPop", key: "covarpop", mro: []Kind{KCovarPop, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"COVAR_POP"}},
	KCovarSamp:                           {name: "CovarSamp", key: "covarsamp", mro: []Kind{KCovarSamp, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"COVAR_SAMP"}},
	KCumeDist:                            {name: "CumeDist", key: "cumedist", mro: []Kind{KCumeDist, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"CUME_DIST"}},
	KDenseRank:                           {name: "DenseRank", key: "denserank", mro: []Kind{KDenseRank, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"DENSE_RANK"}},
	KFirst:                               {name: "First", key: "first", mro: []Kind{KFirst, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"FIRST"}},
	KFirstValue:                          {name: "FirstValue", key: "firstvalue", mro: []Kind{KFirstValue, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"FIRST_VALUE"}},
	KGroupConcat:                         {name: "GroupConcat", key: "groupconcat", mro: []Kind{KGroupConcat, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"separator", false}, {"on_overflow", false}}, flags: 0, sqlNames: []string{"GROUP_CONCAT"}},
	KGrouping:                            {name: "Grouping", key: "grouping", mro: []Kind{KGrouping, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"GROUPING"}},
	KGroupingId:                          {name: "GroupingId", key: "groupingid", mro: []Kind{KGroupingId, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"GROUPING_ID"}},
	KKurtosis:                            {name: "Kurtosis", key: "kurtosis", mro: []Kind{KKurtosis, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"KURTOSIS"}},
	KLag:                                 {name: "Lag", key: "lag", mro: []Kind{KLag, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"offset", false}, {"default", false}}, flags: 0, sqlNames: []string{"LAG"}},
	KLast:                                {name: "Last", key: "last", mro: []Kind{KLast, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"LAST"}},
	KLastValue:                           {name: "LastValue", key: "lastvalue", mro: []Kind{KLastValue, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"LAST_VALUE"}},
	KLead:                                {name: "Lead", key: "lead", mro: []Kind{KLead, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"offset", false}, {"default", false}}, flags: 0, sqlNames: []string{"LEAD"}},
	KLogicalAnd:                          {name: "LogicalAnd", key: "logicaland", mro: []Kind{KLogicalAnd, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"LOGICAL_AND", "BOOL_AND", "BOOLAND_AGG"}},
	KLogicalOr:                           {name: "LogicalOr", key: "logicalor", mro: []Kind{KLogicalOr, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"LOGICAL_OR", "BOOL_OR", "BOOLOR_AGG"}},
	KMax:                                 {name: "Max", key: "max", mro: []Kind{KMax, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"count", false}}, flags: kfVarLenArgs, sqlNames: []string{"MAX"}},
	KMedian:                              {name: "Median", key: "median", mro: []Kind{KMedian, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"MEDIAN"}},
	KMin:                                 {name: "Min", key: "min", mro: []Kind{KMin, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"count", false}}, flags: kfVarLenArgs, sqlNames: []string{"MIN"}},
	KMinhash:                             {name: "Minhash", key: "minhash", mro: []Kind{KMinhash, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"MINHASH"}},
	KMinhashCombine:                      {name: "MinhashCombine", key: "minhashcombine", mro: []Kind{KMinhashCombine, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"MINHASH_COMBINE"}},
	KMode:                                {name: "Mode", key: "mode", mro: []Kind{KMode, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"deterministic", false}}, flags: 0, sqlNames: []string{"MODE"}},
	KNtile:                               {name: "Ntile", key: "ntile", mro: []Kind{KNtile, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"NTILE"}},
	KNthValue:                            {name: "NthValue", key: "nthvalue", mro: []Kind{KNthValue, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"offset", true}, {"from_first", false}}, flags: 0, sqlNames: []string{"NTH_VALUE"}},
	KObjectAgg:                           {name: "ObjectAgg", key: "objectagg", mro: []Kind{KObjectAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"OBJECT_AGG"}},
	KPercentileCont:                      {name: "PercentileCont", key: "percentilecont", mro: []Kind{KPercentileCont, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"PERCENTILE_CONT"}},
	KPercentileDisc:                      {name: "PercentileDisc", key: "percentiledisc", mro: []Kind{KPercentileDisc, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"PERCENTILE_DISC"}},
	KPercentRank:                         {name: "PercentRank", key: "percentrank", mro: []Kind{KPercentRank, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"PERCENT_RANK"}},
	KQuantile:                            {name: "Quantile", key: "quantile", mro: []Kind{KQuantile, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"quantile", true}}, flags: 0, sqlNames: []string{"QUANTILE"}},
	KApproxQuantile:                      {name: "ApproxQuantile", key: "approxquantile", mro: []Kind{KApproxQuantile, KQuantile, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"quantile", true}, {"accuracy", false}, {"weight", false}, {"error_tolerance", false}}, flags: 0, sqlNames: []string{"APPROX_QUANTILE"}},
	KRank:                                {name: "Rank", key: "rank", mro: []Kind{KRank, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"RANK"}},
	KRegrAvgx:                            {name: "RegrAvgx", key: "regravgx", mro: []Kind{KRegrAvgx, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"REGR_AVGX"}},
	KRegrAvgy:                            {name: "RegrAvgy", key: "regravgy", mro: []Kind{KRegrAvgy, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"REGR_AVGY"}},
	KRegrCount:                           {name: "RegrCount", key: "regrcount", mro: []Kind{KRegrCount, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"REGR_COUNT"}},
	KRegrIntercept:                       {name: "RegrIntercept", key: "regrintercept", mro: []Kind{KRegrIntercept, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"REGR_INTERCEPT"}},
	KRegrR2:                              {name: "RegrR2", key: "regrr2", mro: []Kind{KRegrR2, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"REGR_R2"}},
	KRegrSlope:                           {name: "RegrSlope", key: "regrslope", mro: []Kind{KRegrSlope, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"REGR_SLOPE"}},
	KRegrSxx:                             {name: "RegrSxx", key: "regrsxx", mro: []Kind{KRegrSxx, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"REGR_SXX"}},
	KRegrSxy:                             {name: "RegrSxy", key: "regrsxy", mro: []Kind{KRegrSxy, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"REGR_SXY"}},
	KRegrSyy:                             {name: "RegrSyy", key: "regrsyy", mro: []Kind{KRegrSyy, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"REGR_SYY"}},
	KRegrValx:                            {name: "RegrValx", key: "regrvalx", mro: []Kind{KRegrValx, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"REGR_VALX"}},
	KRegrValy:                            {name: "RegrValy", key: "regrvaly", mro: []Kind{KRegrValy, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"REGR_VALY"}},
	KRowNumber:                           {name: "RowNumber", key: "rownumber", mro: []Kind{KRowNumber, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"ROW_NUMBER"}},
	KSkewness:                            {name: "Skewness", key: "skewness", mro: []Kind{KSkewness, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SKEWNESS"}},
	KStddev:                              {name: "Stddev", key: "stddev", mro: []Kind{KStddev, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"STDDEV", "STDEV"}},
	KStddevPop:                           {name: "StddevPop", key: "stddevpop", mro: []Kind{KStddevPop, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"STDDEV_POP"}},
	KStddevSamp:                          {name: "StddevSamp", key: "stddevsamp", mro: []Kind{KStddevSamp, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"STDDEV_SAMP"}},
	KSum:                                 {name: "Sum", key: "sum", mro: []Kind{KSum, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SUM"}},
	KVariance:                            {name: "Variance", key: "variance", mro: []Kind{KVariance, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"single_value_returns_zero", false}}, flags: 0, sqlNames: []string{"VARIANCE", "VARIANCE_SAMP", "VAR_SAMP"}},
	KVariancePop:                         {name: "VariancePop", key: "variancepop", mro: []Kind{KVariancePop, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"VARIANCE_POP", "VAR_POP"}},
	KArray:                               {name: "Array", key: "array", mro: []Kind{KArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}, {"bracket_notation", false}, {"struct_name_inheritance", false}, {"value_constructor", false}}, flags: kfVarLenArgs, sqlNames: []string{"ARRAY"}},
	KArrayConstructCompact:               {name: "ArrayConstructCompact", key: "arrayconstructcompact", mro: []Kind{KArrayConstructCompact, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"ARRAY_CONSTRUCT_COMPACT"}},
	KList:                                {name: "List", key: "list", mro: []Kind{KList, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"LIST"}},
	KToArray:                             {name: "ToArray", key: "toarray", mro: []Kind{KToArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TO_ARRAY"}},
	KArrayAppend:                         {name: "ArrayAppend", key: "arrayappend", mro: []Kind{KArrayAppend, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"null_propagation", false}}, flags: 0, sqlNames: []string{"ARRAY_APPEND"}},
	KArrayCompact:                        {name: "ArrayCompact", key: "arraycompact", mro: []Kind{KArrayCompact, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ARRAY_COMPACT"}},
	KArrayConcat:                         {name: "ArrayConcat", key: "arrayconcat", mro: []Kind{KArrayConcat, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"null_propagation", false}}, flags: kfVarLenArgs, sqlNames: []string{"ARRAY_CONCAT", "ARRAY_CAT"}},
	KArrayFilter:                         {name: "ArrayFilter", key: "arrayfilter", mro: []Kind{KArrayFilter, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"FILTER", "ARRAY_FILTER"}},
	KArrayInsert:                         {name: "ArrayInsert", key: "arrayinsert", mro: []Kind{KArrayInsert, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"position", true}, {"expression", true}, {"offset", false}}, flags: 0, sqlNames: []string{"ARRAY_INSERT"}},
	KArrayPrepend:                        {name: "ArrayPrepend", key: "arrayprepend", mro: []Kind{KArrayPrepend, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"null_propagation", false}}, flags: 0, sqlNames: []string{"ARRAY_PREPEND"}},
	KArrayRemove:                         {name: "ArrayRemove", key: "arrayremove", mro: []Kind{KArrayRemove, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"null_propagation", false}}, flags: 0, sqlNames: []string{"ARRAY_REMOVE"}},
	KArrayRemoveAt:                       {name: "ArrayRemoveAt", key: "arrayremoveat", mro: []Kind{KArrayRemoveAt, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"position", true}}, flags: 0, sqlNames: []string{"ARRAY_REMOVE_AT"}},
	KArrayReverse:                        {name: "ArrayReverse", key: "arrayreverse", mro: []Kind{KArrayReverse, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ARRAY_REVERSE"}},
	KArraySlice:                          {name: "ArraySlice", key: "arrayslice", mro: []Kind{KArraySlice, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"start", true}, {"end", false}, {"step", false}, {"zero_based", false}}, flags: 0, sqlNames: []string{"ARRAY_SLICE"}},
	KArraySort:                           {name: "ArraySort", key: "arraysort", mro: []Kind{KArraySort, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"ARRAY_SORT"}},
	KSortArray:                           {name: "SortArray", key: "sortarray", mro: []Kind{KSortArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"asc", false}, {"nulls_first", false}}, flags: 0, sqlNames: []string{"SORT_ARRAY"}},
	KArrayAll:                            {name: "ArrayAll", key: "arrayall", mro: []Kind{KArrayAll, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"ARRAY_ALL"}},
	KArrayAny:                            {name: "ArrayAny", key: "arrayany", mro: []Kind{KArrayAny, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"ARRAY_ANY"}},
	KArrayContains:                       {name: "ArrayContains", key: "arraycontains", mro: []Kind{KArrayContains, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"ensure_variant", false}, {"check_null", false}}, flags: 0, sqlNames: []string{"ARRAY_CONTAINS", "ARRAY_HAS"}},
	KArrayContainsAll:                    {name: "ArrayContainsAll", key: "arraycontainsall", mro: []Kind{KArrayContainsAll, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"ARRAY_CONTAINS_ALL", "ARRAY_HAS_ALL"}},
	KArrayContainedBy:                    {name: "ArrayContainedBy", key: "arraycontainedby", mro: []Kind{KArrayContainedBy, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"ARRAY_CONTAINED_BY"}},
	KArrayExcept:                         {name: "ArrayExcept", key: "arrayexcept", mro: []Kind{KArrayExcept, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"is_multiset", false}}, flags: 0, sqlNames: []string{"ARRAY_EXCEPT"}},
	KArrayIntersect:                      {name: "ArrayIntersect", key: "arrayintersect", mro: []Kind{KArrayIntersect, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}, {"is_multiset", false}}, flags: kfVarLenArgs, sqlNames: []string{"ARRAY_INTERSECT", "ARRAY_INTERSECTION"}},
	KArrayOverlaps:                       {name: "ArrayOverlaps", key: "arrayoverlaps", mro: []Kind{KArrayOverlaps, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"null_safe", false}}, flags: 0, sqlNames: []string{"ARRAY_OVERLAPS"}},
	KArrayPosition:                       {name: "ArrayPosition", key: "arrayposition", mro: []Kind{KArrayPosition, KExpression, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"zero_based", false}}, flags: 0, sqlNames: []string{"ARRAY_POSITION"}},
	KArrayDistinct:                       {name: "ArrayDistinct", key: "arraydistinct", mro: []Kind{KArrayDistinct, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"check_null", false}}, flags: 0, sqlNames: []string{"ARRAY_DISTINCT"}},
	KArrayFirst:                          {name: "ArrayFirst", key: "arrayfirst", mro: []Kind{KArrayFirst, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"ARRAY_FIRST"}},
	KArrayLast:                           {name: "ArrayLast", key: "arraylast", mro: []Kind{KArrayLast, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ARRAY_LAST"}},
	KArrayMax:                            {name: "ArrayMax", key: "arraymax", mro: []Kind{KArrayMax, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ARRAY_MAX"}},
	KArrayMin:                            {name: "ArrayMin", key: "arraymin", mro: []Kind{KArrayMin, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ARRAY_MIN"}},
	KArraySize:                           {name: "ArraySize", key: "arraysize", mro: []Kind{KArraySize, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"ARRAY_SIZE", "ARRAY_LENGTH"}},
	KArraySum:                            {name: "ArraySum", key: "arraysum", mro: []Kind{KArraySum, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"ARRAY_SUM"}},
	KArraysZip:                           {name: "ArraysZip", key: "arrayszip", mro: []Kind{KArraysZip, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"ARRAYS_ZIP"}},
	KArrayToString:                       {name: "ArrayToString", key: "arraytostring", mro: []Kind{KArrayToString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"null", false}, {"null_is_empty", false}, {"null_delim_is_null", false}}, flags: 0, sqlNames: []string{"ARRAY_TO_STRING", "ARRAY_JOIN"}},
	KFlatten:                             {name: "Flatten", key: "flatten", mro: []Kind{KFlatten, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"FLATTEN"}},
	KStringToArray:                       {name: "StringToArray", key: "stringtoarray", mro: []Kind{KStringToArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"null", false}}, flags: 0, sqlNames: []string{"STRING_TO_ARRAY", "SPLIT_BY_STRING"}},
	KStrtokToArray:                       {name: "StrtokToArray", key: "strtoktoarray", mro: []Kind{KStrtokToArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"STRTOK_TO_ARRAY"}},
	KApply:                               {name: "Apply", key: "apply", mro: []Kind{KApply, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"APPLY"}},
	KReduce:                              {name: "Reduce", key: "reduce", mro: []Kind{KReduce, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"initial", true}, {"merge", true}, {"finish", false}}, flags: 0, sqlNames: []string{"REDUCE"}},
	KTransform:                           {name: "Transform", key: "transform", mro: []Kind{KTransform, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"TRANSFORM"}},
	KGenerateSeries:                      {name: "GenerateSeries", key: "generateseries", mro: []Kind{KGenerateSeries, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"start", true}, {"end", true}, {"step", false}, {"is_end_exclusive", false}}, flags: 0, sqlNames: []string{"GENERATE_SERIES"}},
	KExplodingGenerateSeries:             {name: "ExplodingGenerateSeries", key: "explodinggenerateseries", mro: []Kind{KExplodingGenerateSeries, KGenerateSeries, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"start", true}, {"end", true}, {"step", false}, {"is_end_exclusive", false}}, flags: 0, sqlNames: []string{"EXPLODING_GENERATE_SERIES"}},
	KGenerator:                           {name: "Generator", key: "generator", mro: []Kind{KGenerator, KExpression, KFunc, KCondition, KUDTF, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"rowcount", false}, {"timelimit", false}}, flags: 0, sqlNames: []string{"GENERATOR"}},
	KExplode:                             {name: "Explode", key: "explode", mro: []Kind{KExplode, KExpression, KFunc, KCondition, KUDTF, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"EXPLODE"}},
	KInline:                              {name: "Inline", key: "inline", mro: []Kind{KInline, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"INLINE"}},
	KExplodeOuter:                        {name: "ExplodeOuter", key: "explodeouter", mro: []Kind{KExplodeOuter, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	K_ExplodeOuter:                       {name: "_ExplodeOuter", key: "_explodeouter", mro: []Kind{K_ExplodeOuter, KExplode, KExpression, KFunc, KCondition, KUDTF, KDerivedTable, KSelectable, KExplodeOuter, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"EXPLODE_OUTER"}},
	KPosexplode:                          {name: "Posexplode", key: "posexplode", mro: []Kind{KPosexplode, KExplode, KExpression, KFunc, KCondition, KUDTF, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"POSEXPLODE"}},
	KPosexplodeOuter:                     {name: "PosexplodeOuter", key: "posexplodeouter", mro: []Kind{KPosexplodeOuter, KPosexplode, KExplode, KExpression, KFunc, KCondition, KUDTF, KDerivedTable, KSelectable, KExplodeOuter, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"POSEXPLODE_OUTER"}},
	KSplitToTable:                        {name: "SplitToTable", key: "splittotable", mro: []Kind{KSplitToTable, KExpression, KFunc, KCondition, KUDTF, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"SPLIT_TO_TABLE"}},
	KPositionalColumn:                    {name: "PositionalColumn", key: "positionalcolumn", mro: []Kind{KPositionalColumn, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KUnnest:                              {name: "Unnest", key: "unnest", mro: []Kind{KUnnest, KExpression, KFunc, KCondition, KUDTF, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"expressions", true}, {"alias", false}, {"offset", false}, {"explode_array", false}}, flags: 0, sqlNames: []string{"UNNEST"}},
	KMap:                                 {name: "Map", key: "map", mro: []Kind{KMap, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"keys", false}, {"values", false}}, flags: 0, sqlNames: []string{"MAP"}},
	KMapCat:                              {name: "MapCat", key: "mapcat", mro: []Kind{KMapCat, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"MAP_CAT"}},
	KMapContainsKey:                      {name: "MapContainsKey", key: "mapcontainskey", mro: []Kind{KMapContainsKey, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"key", true}}, flags: 0, sqlNames: []string{"MAP_CONTAINS_KEY"}},
	KMapDelete:                           {name: "MapDelete", key: "mapdelete", mro: []Kind{KMapDelete, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"MAP_DELETE"}},
	KMapFromEntries:                      {name: "MapFromEntries", key: "mapfromentries", mro: []Kind{KMapFromEntries, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"MAP_FROM_ENTRIES"}},
	KMapInsert:                           {name: "MapInsert", key: "mapinsert", mro: []Kind{KMapInsert, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"key", false}, {"value", true}, {"update_flag", false}}, flags: 0, sqlNames: []string{"MAP_INSERT"}},
	KMapKeys:                             {name: "MapKeys", key: "mapkeys", mro: []Kind{KMapKeys, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"MAP_KEYS"}},
	KMapPick:                             {name: "MapPick", key: "mappick", mro: []Kind{KMapPick, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"MAP_PICK"}},
	KMapSize:                             {name: "MapSize", key: "mapsize", mro: []Kind{KMapSize, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"MAP_SIZE"}},
	KStarMap:                             {name: "StarMap", key: "starmap", mro: []Kind{KStarMap, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"STAR_MAP"}},
	KToMap:                               {name: "ToMap", key: "tomap", mro: []Kind{KToMap, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TO_MAP"}},
	KVarMap:                              {name: "VarMap", key: "varmap", mro: []Kind{KVarMap, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"keys", true}, {"values", true}}, flags: kfVarLenArgs, sqlNames: []string{"VAR_MAP"}},
	KStruct:                              {name: "Struct", key: "struct", mro: []Kind{KStruct, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"STRUCT"}},
	KStructExtract:                       {name: "StructExtract", key: "structextract", mro: []Kind{KStructExtract, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"STRUCT_EXTRACT"}},
	KStDistance:                          {name: "StDistance", key: "stdistance", mro: []Kind{KStDistance, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"use_spheroid", false}}, flags: 0, sqlNames: []string{"ST_DISTANCE"}},
	KStPoint:                             {name: "StPoint", key: "stpoint", mro: []Kind{KStPoint, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"z", false}, {"m", false}}, flags: 0, sqlNames: []string{"ST_POINT", "ST_MAKEPOINT"}},
	KIndexConstraintOption:               {name: "IndexConstraintOption", key: "indexconstraintoption", mro: []Kind{KIndexConstraintOption, KExpression, KExpr}, args: []argSpec{{"key_block_size", false}, {"using", false}, {"parser", false}, {"comment", false}, {"visible", false}, {"engine_attr", false}, {"secondary_engine_attr", false}}, flags: 0, sqlNames: []string{}},
	KReference:                           {name: "Reference", key: "reference", mro: []Kind{KReference, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"options", false}}, flags: 0, sqlNames: []string{}},
	KColumnConstraint:                    {name: "ColumnConstraint", key: "columnconstraint", mro: []Kind{KColumnConstraint, KExpression, KExpr}, args: []argSpec{{"this", false}, {"kind", true}}, flags: 0, sqlNames: []string{}},
	KAutoIncrementColumnConstraint:       {name: "AutoIncrementColumnConstraint", key: "autoincrementcolumnconstraint", mro: []Kind{KAutoIncrementColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KInvisibleColumnConstraint:           {name: "InvisibleColumnConstraint", key: "invisiblecolumnconstraint", mro: []Kind{KInvisibleColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KZeroFillColumnConstraint:            {name: "ZeroFillColumnConstraint", key: "zerofillcolumnconstraint", mro: []Kind{KZeroFillColumnConstraint, KColumnConstraint, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KPeriodForSystemTimeConstraint:       {name: "PeriodForSystemTimeConstraint", key: "periodforsystemtimeconstraint", mro: []Kind{KPeriodForSystemTimeConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KCaseSpecificColumnConstraint:        {name: "CaseSpecificColumnConstraint", key: "casespecificcolumnconstraint", mro: []Kind{KCaseSpecificColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"not_", true}}, flags: 0, sqlNames: []string{}},
	KCharacterSetColumnConstraint:        {name: "CharacterSetColumnConstraint", key: "charactersetcolumnconstraint", mro: []Kind{KCharacterSetColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KCheckColumnConstraint:               {name: "CheckColumnConstraint", key: "checkcolumnconstraint", mro: []Kind{KCheckColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}, {"enforced", false}}, flags: 0, sqlNames: []string{}},
	KAssumeColumnConstraint:              {name: "AssumeColumnConstraint", key: "assumecolumnconstraint", mro: []Kind{KAssumeColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KClusteredColumnConstraint:           {name: "ClusteredColumnConstraint", key: "clusteredcolumnconstraint", mro: []Kind{KClusteredColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KCollateColumnConstraint:             {name: "CollateColumnConstraint", key: "collatecolumnconstraint", mro: []Kind{KCollateColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KCommentColumnConstraint:             {name: "CommentColumnConstraint", key: "commentcolumnconstraint", mro: []Kind{KCommentColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KCompressColumnConstraint:            {name: "CompressColumnConstraint", key: "compresscolumnconstraint", mro: []Kind{KCompressColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KDateFormatColumnConstraint:          {name: "DateFormatColumnConstraint", key: "dateformatcolumnconstraint", mro: []Kind{KDateFormatColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KDefaultColumnConstraint:             {name: "DefaultColumnConstraint", key: "defaultcolumnconstraint", mro: []Kind{KDefaultColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KEncodeColumnConstraint:              {name: "EncodeColumnConstraint", key: "encodecolumnconstraint", mro: []Kind{KEncodeColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KExcludeColumnConstraint:             {name: "ExcludeColumnConstraint", key: "excludecolumnconstraint", mro: []Kind{KExcludeColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KEphemeralColumnConstraint:           {name: "EphemeralColumnConstraint", key: "ephemeralcolumnconstraint", mro: []Kind{KEphemeralColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KWithOperator:                        {name: "WithOperator", key: "withoperator", mro: []Kind{KWithOperator, KExpression, KExpr}, args: []argSpec{{"this", true}, {"op", true}}, flags: 0, sqlNames: []string{}},
	KGeneratedAsIdentityColumnConstraint: {name: "GeneratedAsIdentityColumnConstraint", key: "generatedasidentitycolumnconstraint", mro: []Kind{KGeneratedAsIdentityColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", false}, {"expression", false}, {"on_null", false}, {"start", false}, {"increment", false}, {"minvalue", false}, {"maxvalue", false}, {"cycle", false}, {"order", false}}, flags: 0, sqlNames: []string{}},
	KGeneratedAsRowColumnConstraint:      {name: "GeneratedAsRowColumnConstraint", key: "generatedasrowcolumnconstraint", mro: []Kind{KGeneratedAsRowColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"start", false}, {"hidden", false}}, flags: 0, sqlNames: []string{}},
	KIndexColumnConstraint:               {name: "IndexColumnConstraint", key: "indexcolumnconstraint", mro: []Kind{KIndexColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}, {"kind", false}, {"index_type", false}, {"options", false}, {"expression", false}, {"granularity", false}}, flags: 0, sqlNames: []string{}},
	KInlineLengthColumnConstraint:        {name: "InlineLengthColumnConstraint", key: "inlinelengthcolumnconstraint", mro: []Kind{KInlineLengthColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KNonClusteredColumnConstraint:        {name: "NonClusteredColumnConstraint", key: "nonclusteredcolumnconstraint", mro: []Kind{KNonClusteredColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KNotForReplicationColumnConstraint:   {name: "NotForReplicationColumnConstraint", key: "notforreplicationcolumnconstraint", mro: []Kind{KNotForReplicationColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KMaskingPolicyColumnConstraint:       {name: "MaskingPolicyColumnConstraint", key: "maskingpolicycolumnconstraint", mro: []Kind{KMaskingPolicyColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KNotNullColumnConstraint:             {name: "NotNullColumnConstraint", key: "notnullcolumnconstraint", mro: []Kind{KNotNullColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"allow_null", false}}, flags: 0, sqlNames: []string{}},
	KOnUpdateColumnConstraint:            {name: "OnUpdateColumnConstraint", key: "onupdatecolumnconstraint", mro: []Kind{KOnUpdateColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KPrimaryKeyColumnConstraint:          {name: "PrimaryKeyColumnConstraint", key: "primarykeycolumnconstraint", mro: []Kind{KPrimaryKeyColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"desc", false}, {"options", false}, {"include", false}}, flags: 0, sqlNames: []string{}},
	KTitleColumnConstraint:               {name: "TitleColumnConstraint", key: "titlecolumnconstraint", mro: []Kind{KTitleColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KUniqueColumnConstraint:              {name: "UniqueColumnConstraint", key: "uniquecolumnconstraint", mro: []Kind{KUniqueColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", false}, {"index_type", false}, {"on_conflict", false}, {"nulls", false}, {"options", false}, {"include", false}}, flags: 0, sqlNames: []string{}},
	KUppercaseColumnConstraint:           {name: "UppercaseColumnConstraint", key: "uppercasecolumnconstraint", mro: []Kind{KUppercaseColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KWatermarkColumnConstraint:           {name: "WatermarkColumnConstraint", key: "watermarkcolumnconstraint", mro: []Kind{KWatermarkColumnConstraint, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KPathColumnConstraint:                {name: "PathColumnConstraint", key: "pathcolumnconstraint", mro: []Kind{KPathColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KProjectionPolicyColumnConstraint:    {name: "ProjectionPolicyColumnConstraint", key: "projectionpolicycolumnconstraint", mro: []Kind{KProjectionPolicyColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KComputedColumnConstraint:            {name: "ComputedColumnConstraint", key: "computedcolumnconstraint", mro: []Kind{KComputedColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}, {"persisted", false}, {"not_null", false}, {"data_type", false}}, flags: 0, sqlNames: []string{}},
	KInOutColumnConstraint:               {name: "InOutColumnConstraint", key: "inoutcolumnconstraint", mro: []Kind{KInOutColumnConstraint, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"input_", false}, {"output", false}, {"variadic", false}}, flags: 0, sqlNames: []string{}},
	KConstraint:                          {name: "Constraint", key: "constraint", mro: []Kind{KConstraint, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: 0, sqlNames: []string{}},
	KForeignKey:                          {name: "ForeignKey", key: "foreignkey", mro: []Kind{KForeignKey, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}, {"reference", false}, {"delete", false}, {"update", false}, {"options", false}}, flags: 0, sqlNames: []string{}},
	KColumnPrefix:                        {name: "ColumnPrefix", key: "columnprefix", mro: []Kind{KColumnPrefix, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KPrimaryKey:                          {name: "PrimaryKey", key: "primarykey", mro: []Kind{KPrimaryKey, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", true}, {"options", false}, {"include", false}}, flags: 0, sqlNames: []string{}},
	KTimeseriesKey:                       {name: "TimeseriesKey", key: "timeserieskey", mro: []Kind{KTimeseriesKey, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KIndexParameters:                     {name: "IndexParameters", key: "indexparameters", mro: []Kind{KIndexParameters, KExpression, KExpr}, args: []argSpec{{"using", false}, {"include", false}, {"columns", false}, {"with_storage", false}, {"partition_by", false}, {"tablespace", false}, {"where", false}, {"on", false}}, flags: 0, sqlNames: []string{}},
	KAddConstraint:                       {name: "AddConstraint", key: "addconstraint", mro: []Kind{KAddConstraint, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KExpr:                                {name: "Expr", key: "expression", mro: []Kind{KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KExpression:                          {name: "Expression", key: "expression", mro: []Kind{KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KCondition:                           {name: "Condition", key: "condition", mro: []Kind{KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KPredicate:                           {name: "Predicate", key: "predicate", mro: []Kind{KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KCache:                               {name: "Cache", key: "cache", mro: []Kind{KCache, KExpression, KExpr}, args: []argSpec{{"this", true}, {"lazy", false}, {"options", false}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KUncache:                             {name: "Uncache", key: "uncache", mro: []Kind{KUncache, KExpression, KExpr}, args: []argSpec{{"this", true}, {"exists", false}}, flags: 0, sqlNames: []string{}},
	KRefresh:                             {name: "Refresh", key: "refresh", mro: []Kind{KRefresh, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", true}, {"partition_start", false}, {"partition_end", false}, {"force", false}, {"mode", false}}, flags: 0, sqlNames: []string{}},
	KLockingStatement:                    {name: "LockingStatement", key: "lockingstatement", mro: []Kind{KLockingStatement, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KColumnConstraintKind:                {name: "ColumnConstraintKind", key: "columnconstraintkind", mro: []Kind{KColumnConstraintKind, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KSubqueryPredicate:                   {name: "SubqueryPredicate", key: "subquerypredicate", mro: []Kind{KSubqueryPredicate, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KAll:                                 {name: "All", key: "all", mro: []Kind{KAll, KExpression, KSubqueryPredicate, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KAny:                                 {name: "Any", key: "any", mro: []Kind{KAny, KExpression, KSubqueryPredicate, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KBinary:                              {name: "Binary", key: "binary", mro: []Kind{KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KConnector:                           {name: "Connector", key: "connector", mro: []Kind{KConnector, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KFunc:                                {name: "Func", key: "func", mro: []Kind{KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KAggFunc:                             {name: "AggFunc", key: "aggfunc", mro: []Kind{KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"AGG_FUNC"}},
	KColumn:                              {name: "Column", key: "column", mro: []Kind{KColumn, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"table", false}, {"db", false}, {"catalog", false}, {"join_mark", false}}, flags: 0, sqlNames: []string{}},
	KLiteral:                             {name: "Literal", key: "literal", mro: []Kind{KLiteral, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"is_string", true}}, flags: kfHashRawArgs | kfPrimitive, sqlNames: []string{}},
	KVar:                                 {name: "Var", key: "var", mro: []Kind{KVar, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: kfPrimitive, sqlNames: []string{}},
	KWithinGroup:                         {name: "WithinGroup", key: "withingroup", mro: []Kind{KWithinGroup, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KPseudocolumn:                        {name: "Pseudocolumn", key: "pseudocolumn", mro: []Kind{KPseudocolumn, KColumn, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"table", false}, {"db", false}, {"catalog", false}, {"join_mark", false}}, flags: 0, sqlNames: []string{}},
	KHint:                                {name: "Hint", key: "hint", mro: []Kind{KHint, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KJoinHint:                            {name: "JoinHint", key: "joinhint", mro: []Kind{KJoinHint, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: 0, sqlNames: []string{}},
	KIdentifier:                          {name: "Identifier", key: "identifier", mro: []Kind{KIdentifier, KExpression, KExpr}, args: []argSpec{{"this", true}, {"quoted", false}, {"global_", false}, {"temporary", false}}, flags: kfHashRawArgs | kfPrimitive, sqlNames: []string{}},
	KDynamicIdentifier:                   {name: "DynamicIdentifier", key: "dynamicidentifier", mro: []Kind{KDynamicIdentifier, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: 0, sqlNames: []string{"DYNAMIC_IDENTIFIER"}},
	KOpclass:                             {name: "Opclass", key: "opclass", mro: []Kind{KOpclass, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KStar:                                {name: "Star", key: "star", mro: []Kind{KStar, KExpression, KExpr}, args: []argSpec{{"except_", false}, {"replace", false}, {"rename", false}, {"ilike", false}}, flags: 0, sqlNames: []string{}},
	KParameter:                           {name: "Parameter", key: "parameter", mro: []Kind{KParameter, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KSessionParameter:                    {name: "SessionParameter", key: "sessionparameter", mro: []Kind{KSessionParameter, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"kind", false}}, flags: 0, sqlNames: []string{}},
	KPlaceholder:                         {name: "Placeholder", key: "placeholder", mro: []Kind{KPlaceholder, KExpression, KCondition, KExpr}, args: []argSpec{{"this", false}, {"kind", false}, {"widget", false}, {"jdbc", false}}, flags: 0, sqlNames: []string{}},
	KNull:                                {name: "Null", key: "null", mro: []Kind{KNull, KExpression, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KBoolean:                             {name: "Boolean", key: "boolean", mro: []Kind{KBoolean, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: kfPrimitive, sqlNames: []string{}},
	KDot:                                 {name: "Dot", key: "dot", mro: []Kind{KDot, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KKwarg:                               {name: "Kwarg", key: "kwarg", mro: []Kind{KKwarg, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KAlias:                               {name: "Alias", key: "alias", mro: []Kind{KAlias, KExpression, KExpr}, args: []argSpec{{"this", true}, {"alias", false}}, flags: 0, sqlNames: []string{}},
	KPivotAlias:                          {name: "PivotAlias", key: "pivotalias", mro: []Kind{KPivotAlias, KAlias, KExpression, KExpr}, args: []argSpec{{"this", true}, {"alias", false}}, flags: 0, sqlNames: []string{}},
	KPivotAny:                            {name: "PivotAny", key: "pivotany", mro: []Kind{KPivotAny, KExpression, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KAliases:                             {name: "Aliases", key: "aliases", mro: []Kind{KAliases, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: 0, sqlNames: []string{}},
	KBracket:                             {name: "Bracket", key: "bracket", mro: []Kind{KBracket, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}, {"offset", false}, {"safe", false}, {"returns_list_for_maps", false}, {"json_access", false}}, flags: 0, sqlNames: []string{}},
	KForIn:                               {name: "ForIn", key: "forin", mro: []Kind{KForIn, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KIgnoreNulls:                         {name: "IgnoreNulls", key: "ignorenulls", mro: []Kind{KIgnoreNulls, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KRespectNulls:                        {name: "RespectNulls", key: "respectnulls", mro: []Kind{KRespectNulls, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KHavingMax:                           {name: "HavingMax", key: "havingmax", mro: []Kind{KHavingMax, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"max", true}}, flags: 0, sqlNames: []string{}},
	KSafeFunc:                            {name: "SafeFunc", key: "safefunc", mro: []Kind{KSafeFunc, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SAFE_FUNC"}},
	KTypeof:                              {name: "Typeof", key: "typeof", mro: []Kind{KTypeof, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TYPEOF"}},
	KParameterizedAgg:                    {name: "ParameterizedAgg", key: "parameterizedagg", mro: []Kind{KParameterizedAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}, {"params", true}}, flags: 0, sqlNames: []string{"PARAMETERIZED_AGG"}},
	KAnonymous:                           {name: "Anonymous", key: "anonymous", mro: []Kind{KAnonymous, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"ANONYMOUS"}},
	KAnonymousAggFunc:                    {name: "AnonymousAggFunc", key: "anonymousaggfunc", mro: []Kind{KAnonymousAggFunc, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"ANONYMOUS_AGG_FUNC"}},
	KCombinedAggFunc:                     {name: "CombinedAggFunc", key: "combinedaggfunc", mro: []Kind{KCombinedAggFunc, KAnonymousAggFunc, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"COMBINED_AGG_FUNC"}},
	KCombinedParameterizedAgg:            {name: "CombinedParameterizedAgg", key: "combinedparameterizedagg", mro: []Kind{KCombinedParameterizedAgg, KParameterizedAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}, {"params", true}}, flags: 0, sqlNames: []string{"COMBINED_PARAMETERIZED_AGG"}},
	KHashAgg:                             {name: "HashAgg", key: "hashagg", mro: []Kind{KHashAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"HASH_AGG"}},
	KHll:                                 {name: "Hll", key: "hll", mro: []Kind{KHll, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"HLL"}},
	KApproxDistinct:                      {name: "ApproxDistinct", key: "approxdistinct", mro: []Kind{KApproxDistinct, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"accuracy", false}}, flags: 0, sqlNames: []string{"APPROX_DISTINCT", "APPROX_COUNT_DISTINCT"}},
	KSlice:                               {name: "Slice", key: "slice", mro: []Kind{KSlice, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expression", false}, {"step", false}}, flags: 0, sqlNames: []string{}},
	KTimeUnit:                            {name: "TimeUnit", key: "timeunit", mro: []Kind{KTimeUnit, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	K_TimeUnit:                           {name: "_TimeUnit", key: "_timeunit", mro: []Kind{K_TimeUnit, KExpression, KTimeUnit, KExpr}, args: []argSpec{{"unit", false}}, flags: 0, sqlNames: []string{}},
	KIntervalOp:                          {name: "IntervalOp", key: "intervalop", mro: []Kind{KIntervalOp, KTimeUnit, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KFilter:                              {name: "Filter", key: "filter", mro: []Kind{KFilter, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KCheck:                               {name: "Check", key: "check", mro: []Kind{KCheck, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KOrdered:                             {name: "Ordered", key: "ordered", mro: []Kind{KOrdered, KExpression, KExpr}, args: []argSpec{{"this", true}, {"desc", false}, {"nulls_first", true}, {"with_fill", false}}, flags: 0, sqlNames: []string{}},
	KAdd:                                 {name: "Add", key: "add", mro: []Kind{KAdd, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KBitwiseAnd:                          {name: "BitwiseAnd", key: "bitwiseand", mro: []Kind{KBitwiseAnd, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"padside", false}}, flags: 0, sqlNames: []string{}},
	KBitwiseLeftShift:                    {name: "BitwiseLeftShift", key: "bitwiseleftshift", mro: []Kind{KBitwiseLeftShift, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"requires_int128", false}}, flags: 0, sqlNames: []string{}},
	KBitwiseOr:                           {name: "BitwiseOr", key: "bitwiseor", mro: []Kind{KBitwiseOr, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"padside", false}}, flags: 0, sqlNames: []string{}},
	KBitwiseRightShift:                   {name: "BitwiseRightShift", key: "bitwiserightshift", mro: []Kind{KBitwiseRightShift, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"requires_int128", false}}, flags: 0, sqlNames: []string{}},
	KBitwiseXor:                          {name: "BitwiseXor", key: "bitwisexor", mro: []Kind{KBitwiseXor, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"padside", false}}, flags: 0, sqlNames: []string{}},
	KDiv:                                 {name: "Div", key: "div", mro: []Kind{KDiv, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"typed", false}, {"safe", false}}, flags: 0, sqlNames: []string{}},
	KOverlaps:                            {name: "Overlaps", key: "overlaps", mro: []Kind{KOverlaps, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KExtendsLeft:                         {name: "ExtendsLeft", key: "extendsleft", mro: []Kind{KExtendsLeft, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KExtendsRight:                        {name: "ExtendsRight", key: "extendsright", mro: []Kind{KExtendsRight, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KDPipe:                               {name: "DPipe", key: "dpipe", mro: []Kind{KDPipe, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"safe", false}}, flags: 0, sqlNames: []string{}},
	KEQ:                                  {name: "EQ", key: "eq", mro: []Kind{KEQ, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KNullSafeEQ:                          {name: "NullSafeEQ", key: "nullsafeeq", mro: []Kind{KNullSafeEQ, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KNullSafeNEQ:                         {name: "NullSafeNEQ", key: "nullsafeneq", mro: []Kind{KNullSafeNEQ, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KPropertyEQ:                          {name: "PropertyEQ", key: "propertyeq", mro: []Kind{KPropertyEQ, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KDistance:                            {name: "Distance", key: "distance", mro: []Kind{KDistance, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KDistanceNd:                          {name: "DistanceNd", key: "distancend", mro: []Kind{KDistanceNd, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KEscape:                              {name: "Escape", key: "escape", mro: []Kind{KEscape, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KGlob:                                {name: "Glob", key: "glob", mro: []Kind{KGlob, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KGT:                                  {name: "GT", key: "gt", mro: []Kind{KGT, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KGTE:                                 {name: "GTE", key: "gte", mro: []Kind{KGTE, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KILike:                               {name: "ILike", key: "ilike", mro: []Kind{KILike, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"negate", false}}, flags: 0, sqlNames: []string{}},
	KIntDiv:                              {name: "IntDiv", key: "intdiv", mro: []Kind{KIntDiv, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KIs:                                  {name: "Is", key: "is", mro: []Kind{KIs, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"negate", false}}, flags: 0, sqlNames: []string{}},
	KLike:                                {name: "Like", key: "like", mro: []Kind{KLike, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"negate", false}}, flags: 0, sqlNames: []string{}},
	KMatch:                               {name: "Match", key: "match", mro: []Kind{KMatch, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"kind", false}, {"analyzer", false}}, flags: 0, sqlNames: []string{}},
	KLT:                                  {name: "LT", key: "lt", mro: []Kind{KLT, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KLTE:                                 {name: "LTE", key: "lte", mro: []Kind{KLTE, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KMod:                                 {name: "Mod", key: "mod", mro: []Kind{KMod, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KMul:                                 {name: "Mul", key: "mul", mro: []Kind{KMul, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KNEQ:                                 {name: "NEQ", key: "neq", mro: []Kind{KNEQ, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KNestedJSONSelect:                    {name: "NestedJSONSelect", key: "nestedjsonselect", mro: []Kind{KNestedJSONSelect, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KOperator:                            {name: "Operator", key: "operator", mro: []Kind{KOperator, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"operator", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KSimilarTo:                           {name: "SimilarTo", key: "similarto", mro: []Kind{KSimilarTo, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KSub:                                 {name: "Sub", key: "sub", mro: []Kind{KSub, KExpression, KBinary, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KAdjacent:                            {name: "Adjacent", key: "adjacent", mro: []Kind{KAdjacent, KExpression, KBinary, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KUnary:                               {name: "Unary", key: "unary", mro: []Kind{KUnary, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KBitwiseNot:                          {name: "BitwiseNot", key: "bitwisenot", mro: []Kind{KBitwiseNot, KUnary, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KNot:                                 {name: "Not", key: "not", mro: []Kind{KNot, KUnary, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KParen:                               {name: "Paren", key: "paren", mro: []Kind{KParen, KUnary, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KNeg:                                 {name: "Neg", key: "neg", mro: []Kind{KNeg, KUnary, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KAtIndex:                             {name: "AtIndex", key: "atindex", mro: []Kind{KAtIndex, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KAtTimeZone:                          {name: "AtTimeZone", key: "attimezone", mro: []Kind{KAtTimeZone, KExpression, KExpr}, args: []argSpec{{"this", true}, {"zone", true}}, flags: 0, sqlNames: []string{}},
	KFromTimeZone:                        {name: "FromTimeZone", key: "fromtimezone", mro: []Kind{KFromTimeZone, KExpression, KExpr}, args: []argSpec{{"this", true}, {"zone", true}}, flags: 0, sqlNames: []string{}},
	KFormatPhrase:                        {name: "FormatPhrase", key: "formatphrase", mro: []Kind{KFormatPhrase, KExpression, KExpr}, args: []argSpec{{"this", true}, {"format", true}}, flags: 0, sqlNames: []string{}},
	KBetween:                             {name: "Between", key: "between", mro: []Kind{KBetween, KExpression, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"low", true}, {"high", true}, {"symmetric", false}}, flags: 0, sqlNames: []string{}},
	KDistinct:                            {name: "Distinct", key: "distinct", mro: []Kind{KDistinct, KExpression, KExpr}, args: []argSpec{{"expressions", false}, {"on", false}}, flags: 0, sqlNames: []string{}},
	KIn:                                  {name: "In", key: "in", mro: []Kind{KIn, KExpression, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"query", false}, {"unnest", false}, {"field", false}, {"is_global", false}}, flags: 0, sqlNames: []string{}},
	KAnd:                                 {name: "And", key: "and", mro: []Kind{KAnd, KExpression, KConnector, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"AND"}},
	KOr:                                  {name: "Or", key: "or", mro: []Kind{KOr, KExpression, KConnector, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"OR"}},
	KXor:                                 {name: "Xor", key: "xor", mro: []Kind{KXor, KExpression, KConnector, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"round_input", false}}, flags: 0, sqlNames: []string{"XOR"}},
	KPow:                                 {name: "Pow", key: "pow", mro: []Kind{KPow, KExpression, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"POWER", "POW"}},
	KRegexpLike:                          {name: "RegexpLike", key: "regexplike", mro: []Kind{KRegexpLike, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"flag", false}, {"full_match", false}}, flags: 0, sqlNames: []string{"REGEXP_LIKE"}},
	KDataTypeParam:                       {name: "DataTypeParam", key: "datatypeparam", mro: []Kind{KDataTypeParam, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KDataType:                            {name: "DataType", key: "datatype", mro: []Kind{KDataType, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"nested", false}, {"values", false}, {"kind", false}, {"nullable", false}, {"collate", false}}, flags: kfDataType, sqlNames: []string{}},
	KPseudoType:                          {name: "PseudoType", key: "pseudotype", mro: []Kind{KPseudoType, KDataType, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: kfDataType, sqlNames: []string{}},
	KObjectIdentifier:                    {name: "ObjectIdentifier", key: "objectidentifier", mro: []Kind{KObjectIdentifier, KDataType, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: kfDataType, sqlNames: []string{}},
	KIntervalSpan:                        {name: "IntervalSpan", key: "intervalspan", mro: []Kind{KIntervalSpan, KDataType, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: kfDataType, sqlNames: []string{}},
	KInterval:                            {name: "Interval", key: "interval", mro: []Kind{KInterval, K_TimeUnit, KExpression, KTimeUnit, KExpr}, args: []argSpec{{"this", false}, {"unit", false}, {"fraction_in_string", false}}, flags: 0, sqlNames: []string{}},
	KDDL:                                 {name: "DDL", key: "ddl", mro: []Kind{KDDL, KSelectable, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KCreate:                              {name: "Create", key: "create", mro: []Kind{KCreate, KExpression, KDDL, KSelectable, KExpr}, args: []argSpec{{"with_", false}, {"this", true}, {"kind", true}, {"expression", false}, {"exists", false}, {"properties", false}, {"replace", false}, {"refresh", false}, {"unique", false}, {"indexes", false}, {"no_schema_binding", false}, {"begin", false}, {"clone", false}, {"concurrently", false}, {"clustered", false}}, flags: 0, sqlNames: []string{}},
	KSequenceProperties:                  {name: "SequenceProperties", key: "sequenceproperties", mro: []Kind{KSequenceProperties, KExpression, KExpr}, args: []argSpec{{"increment", false}, {"minvalue", false}, {"maxvalue", false}, {"cache", false}, {"start", false}, {"owned", false}, {"options", false}}, flags: 0, sqlNames: []string{}},
	KTriggerProperties:                   {name: "TriggerProperties", key: "triggerproperties", mro: []Kind{KTriggerProperties, KExpression, KExpr}, args: []argSpec{{"table", true}, {"timing", true}, {"events", true}, {"execute", true}, {"constraint", false}, {"referenced_table", false}, {"deferrable", false}, {"initially", false}, {"referencing", false}, {"for_each", false}, {"when", false}}, flags: 0, sqlNames: []string{}},
	KTriggerExecute:                      {name: "TriggerExecute", key: "triggerexecute", mro: []Kind{KTriggerExecute, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KTriggerEvent:                        {name: "TriggerEvent", key: "triggerevent", mro: []Kind{KTriggerEvent, KExpression, KExpr}, args: []argSpec{{"this", true}, {"columns", false}}, flags: 0, sqlNames: []string{}},
	KTriggerReferencing:                  {name: "TriggerReferencing", key: "triggerreferencing", mro: []Kind{KTriggerReferencing, KExpression, KExpr}, args: []argSpec{{"old", false}, {"new", false}}, flags: 0, sqlNames: []string{}},
	KTruncateTable:                       {name: "TruncateTable", key: "truncatetable", mro: []Kind{KTruncateTable, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"is_database", false}, {"exists", false}, {"only", false}, {"cluster", false}, {"identity", false}, {"option", false}, {"partition", false}}, flags: 0, sqlNames: []string{}},
	KClone:                               {name: "Clone", key: "clone", mro: []Kind{KClone, KExpression, KExpr}, args: []argSpec{{"this", true}, {"shallow", false}, {"copy", false}}, flags: 0, sqlNames: []string{}},
	KDescribe:                            {name: "Describe", key: "describe", mro: []Kind{KDescribe, KExpression, KExpr}, args: []argSpec{{"this", true}, {"style", false}, {"kind", false}, {"properties", false}, {"expressions", false}, {"partition", false}, {"format", false}, {"as_json", false}}, flags: 0, sqlNames: []string{}},
	KAttach:                              {name: "Attach", key: "attach", mro: []Kind{KAttach, KExpression, KExpr}, args: []argSpec{{"this", true}, {"exists", false}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KDetach:                              {name: "Detach", key: "detach", mro: []Kind{KDetach, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", false}, {"exists", false}, {"cluster", false}, {"permanent", false}, {"sync", false}}, flags: 0, sqlNames: []string{}},
	KInstall:                             {name: "Install", key: "install", mro: []Kind{KInstall, KExpression, KExpr}, args: []argSpec{{"this", true}, {"from_", false}, {"force", false}}, flags: 0, sqlNames: []string{}},
	KSummarize:                           {name: "Summarize", key: "summarize", mro: []Kind{KSummarize, KExpression, KExpr}, args: []argSpec{{"this", true}, {"table", false}}, flags: 0, sqlNames: []string{}},
	KKill:                                {name: "Kill", key: "kill", mro: []Kind{KKill, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", false}}, flags: 0, sqlNames: []string{}},
	KPragma:                              {name: "Pragma", key: "pragma", mro: []Kind{KPragma, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KDeclare:                             {name: "Declare", key: "declare", mro: []Kind{KDeclare, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"replace", false}}, flags: 0, sqlNames: []string{}},
	KDeclareItem:                         {name: "DeclareItem", key: "declareitem", mro: []Kind{KDeclareItem, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", false}, {"default", false}}, flags: 0, sqlNames: []string{}},
	KSet:                                 {name: "Set", key: "set", mro: []Kind{KSet, KExpression, KExpr}, args: []argSpec{{"expressions", false}, {"unset", false}, {"tag", false}}, flags: 0, sqlNames: []string{}},
	KHeredoc:                             {name: "Heredoc", key: "heredoc", mro: []Kind{KHeredoc, KExpression, KExpr}, args: []argSpec{{"this", true}, {"tag", false}}, flags: 0, sqlNames: []string{}},
	KSetItem:                             {name: "SetItem", key: "setitem", mro: []Kind{KSetItem, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}, {"kind", false}, {"collate", false}, {"global_", false}}, flags: 0, sqlNames: []string{}},
	KShow:                                {name: "Show", key: "show", mro: []Kind{KShow, KExpression, KExpr}, args: []argSpec{{"this", true}, {"history", false}, {"terse", false}, {"target", false}, {"offset", false}, {"starts_with", false}, {"limit", false}, {"from_", false}, {"like", false}, {"where", false}, {"db", false}, {"scope", false}, {"scope_kind", false}, {"full", false}, {"mutex", false}, {"query", false}, {"channel", false}, {"global_", false}, {"log", false}, {"position", false}, {"types", false}, {"privileges", false}, {"for_table", false}, {"for_group", false}, {"for_user", false}, {"for_role", false}, {"into_outfile", false}, {"json", false}, {"iceberg", false}}, flags: 0, sqlNames: []string{}},
	KUserDefinedFunction:                 {name: "UserDefinedFunction", key: "userdefinedfunction", mro: []Kind{KUserDefinedFunction, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"wrapped", false}}, flags: 0, sqlNames: []string{}},
	KMacroOverloads:                      {name: "MacroOverloads", key: "macrooverloads", mro: []Kind{KMacroOverloads, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KMacroOverload:                       {name: "MacroOverload", key: "macrooverload", mro: []Kind{KMacroOverload, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"is_table", false}}, flags: 0, sqlNames: []string{}},
	KCharacterSet:                        {name: "CharacterSet", key: "characterset", mro: []Kind{KCharacterSet, KExpression, KExpr}, args: []argSpec{{"this", true}, {"default", false}}, flags: 0, sqlNames: []string{}},
	KAlterColumn:                         {name: "AlterColumn", key: "altercolumn", mro: []Kind{KAlterColumn, KExpression, KExpr}, args: []argSpec{{"this", true}, {"dtype", false}, {"collate", false}, {"using", false}, {"default", false}, {"drop", false}, {"comment", false}, {"allow_null", false}, {"visible", false}, {"rename_to", false}, {"exists", false}}, flags: 0, sqlNames: []string{}},
	KModifyColumn:                        {name: "ModifyColumn", key: "modifycolumn", mro: []Kind{KModifyColumn, KExpression, KExpr}, args: []argSpec{{"this", true}, {"rename_from", false}}, flags: 0, sqlNames: []string{}},
	KAlterIndex:                          {name: "AlterIndex", key: "alterindex", mro: []Kind{KAlterIndex, KExpression, KExpr}, args: []argSpec{{"this", true}, {"visible", true}}, flags: 0, sqlNames: []string{}},
	KAlterDistStyle:                      {name: "AlterDistStyle", key: "alterdiststyle", mro: []Kind{KAlterDistStyle, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KAlterSortKey:                        {name: "AlterSortKey", key: "altersortkey", mro: []Kind{KAlterSortKey, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}, {"compound", false}}, flags: 0, sqlNames: []string{}},
	KAlterSet:                            {name: "AlterSet", key: "alterset", mro: []Kind{KAlterSet, KExpression, KExpr}, args: []argSpec{{"expressions", false}, {"option", false}, {"tablespace", false}, {"access_method", false}, {"file_format", false}, {"copy_options", false}, {"tag", false}, {"location", false}, {"serde", false}}, flags: 0, sqlNames: []string{}},
	KRenameColumn:                        {name: "RenameColumn", key: "renamecolumn", mro: []Kind{KRenameColumn, KExpression, KExpr}, args: []argSpec{{"this", true}, {"to", true}, {"exists", false}}, flags: 0, sqlNames: []string{}},
	KAlterRename:                         {name: "AlterRename", key: "alterrename", mro: []Kind{KAlterRename, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KRenameIndex:                         {name: "RenameIndex", key: "renameindex", mro: []Kind{KRenameIndex, KExpression, KExpr}, args: []argSpec{{"this", true}, {"to", true}}, flags: 0, sqlNames: []string{}},
	KAlterModifySqlSecurity:              {name: "AlterModifySqlSecurity", key: "altermodifysqlsecurity", mro: []Kind{KAlterModifySqlSecurity, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KSwapTable:                           {name: "SwapTable", key: "swaptable", mro: []Kind{KSwapTable, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KComment:                             {name: "Comment", key: "comment", mro: []Kind{KComment, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", true}, {"expression", true}, {"exists", false}, {"materialized", false}}, flags: 0, sqlNames: []string{}},
	KComprehension:                       {name: "Comprehension", key: "comprehension", mro: []Kind{KComprehension, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"position", false}, {"iterator", true}, {"condition", false}}, flags: 0, sqlNames: []string{}},
	KMergeTreeTTLAction:                  {name: "MergeTreeTTLAction", key: "mergetreettlaction", mro: []Kind{KMergeTreeTTLAction, KExpression, KExpr}, args: []argSpec{{"this", true}, {"delete", false}, {"recompress", false}, {"to_disk", false}, {"to_volume", false}}, flags: 0, sqlNames: []string{}},
	KMergeTreeTTL:                        {name: "MergeTreeTTL", key: "mergetreettl", mro: []Kind{KMergeTreeTTL, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"where", false}, {"group", false}, {"aggregates", false}}, flags: 0, sqlNames: []string{}},
	KDrop:                                {name: "Drop", key: "drop", mro: []Kind{KDrop, KExpression, KExpr}, args: []argSpec{{"this", false}, {"kind", false}, {"tables", false}, {"expressions", false}, {"exists", false}, {"temporary", false}, {"materialized", false}, {"cascade", false}, {"restrict", false}, {"constraints", false}, {"purge", false}, {"cluster", false}, {"concurrently", false}, {"sync", false}, {"iceberg", false}, {"force", false}}, flags: 0, sqlNames: []string{}},
	KDropPrimaryKey:                      {name: "DropPrimaryKey", key: "dropprimarykey", mro: []Kind{KDropPrimaryKey, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KUndrop:                              {name: "Undrop", key: "undrop", mro: []Kind{KUndrop, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", true}, {"rename", false}}, flags: 0, sqlNames: []string{}},
	KCommand:                             {name: "Command", key: "command", mro: []Kind{KCommand, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KTransaction:                         {name: "Transaction", key: "transaction", mro: []Kind{KTransaction, KExpression, KExpr}, args: []argSpec{{"this", false}, {"modes", false}, {"mark", false}}, flags: 0, sqlNames: []string{}},
	KCommit:                              {name: "Commit", key: "commit", mro: []Kind{KCommit, KExpression, KExpr}, args: []argSpec{{"chain", false}, {"this", false}, {"durability", false}}, flags: 0, sqlNames: []string{}},
	KRollback:                            {name: "Rollback", key: "rollback", mro: []Kind{KRollback, KExpression, KExpr}, args: []argSpec{{"savepoint", false}, {"this", false}}, flags: 0, sqlNames: []string{}},
	KAlter:                               {name: "Alter", key: "alter", mro: []Kind{KAlter, KExpression, KExpr}, args: []argSpec{{"this", false}, {"kind", true}, {"actions", true}, {"exists", false}, {"only", false}, {"options", false}, {"cluster", false}, {"not_valid", false}, {"check", false}, {"cascade", false}, {"iceberg", false}}, flags: 0, sqlNames: []string{}},
	KAlterSession:                        {name: "AlterSession", key: "altersession", mro: []Kind{KAlterSession, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"unset", false}}, flags: 0, sqlNames: []string{}},
	KUse:                                 {name: "Use", key: "use", mro: []Kind{KUse, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}, {"kind", false}}, flags: 0, sqlNames: []string{}},
	KNextValueFor:                        {name: "NextValueFor", key: "nextvaluefor", mro: []Kind{KNextValueFor, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"order", false}}, flags: 0, sqlNames: []string{"NEXT_VALUE_FOR"}},
	KExecute:                             {name: "Execute", key: "execute", mro: []Kind{KExecute, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"return_status", false}}, flags: 0, sqlNames: []string{}},
	KExecuteSql:                          {name: "ExecuteSql", key: "executesql", mro: []Kind{KExecuteSql, KExecute, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"return_status", false}}, flags: 0, sqlNames: []string{}},
	KDML:                                 {name: "DML", key: "dml", mro: []Kind{KDML, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KDelete:                              {name: "Delete", key: "delete", mro: []Kind{KDelete, KExpression, KDML, KExpr}, args: []argSpec{{"with_", false}, {"this", false}, {"using", false}, {"where", false}, {"returning", false}, {"order", false}, {"limit", false}, {"tables", false}, {"cluster", false}, {"hint", false}}, flags: 0, sqlNames: []string{}},
	KExport:                              {name: "Export", key: "export", mro: []Kind{KExport, KExpression, KExpr}, args: []argSpec{{"this", true}, {"connection", false}, {"options", true}}, flags: 0, sqlNames: []string{}},
	KCopyParameter:                       {name: "CopyParameter", key: "copyparameter", mro: []Kind{KCopyParameter, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KCopy:                                {name: "Copy", key: "copy", mro: []Kind{KCopy, KExpression, KDML, KExpr}, args: []argSpec{{"this", true}, {"kind", true}, {"files", false}, {"credentials", false}, {"format", false}, {"params", false}}, flags: 0, sqlNames: []string{}},
	KCredentials:                         {name: "Credentials", key: "credentials", mro: []Kind{KCredentials, KExpression, KExpr}, args: []argSpec{{"credentials", false}, {"encryption", false}, {"storage", false}, {"iam_role", false}, {"region", false}}, flags: 0, sqlNames: []string{}},
	KDirectory:                           {name: "Directory", key: "directory", mro: []Kind{KDirectory, KExpression, KExpr}, args: []argSpec{{"this", true}, {"local", false}, {"row_format", false}}, flags: 0, sqlNames: []string{}},
	KDirectoryStage:                      {name: "DirectoryStage", key: "directorystage", mro: []Kind{KDirectoryStage, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KInsert:                              {name: "Insert", key: "insert", mro: []Kind{KInsert, KExpression, KDDL, KSelectable, KDML, KExpr}, args: []argSpec{{"hint", false}, {"with_", false}, {"is_function", false}, {"this", false}, {"expression", false}, {"conflict", false}, {"returning", false}, {"overwrite", false}, {"exists", false}, {"alternative", false}, {"where", false}, {"ignore", false}, {"by_name", false}, {"stored", false}, {"partition", false}, {"settings", false}, {"source", false}, {"default", false}, {"using", false}}, flags: 0, sqlNames: []string{}},
	KOnConflict:                          {name: "OnConflict", key: "onconflict", mro: []Kind{KOnConflict, KExpression, KExpr}, args: []argSpec{{"duplicate", false}, {"expressions", false}, {"action", false}, {"conflict_keys", false}, {"index_predicate", false}, {"constraint", false}, {"where", false}}, flags: 0, sqlNames: []string{}},
	KReturning:                           {name: "Returning", key: "returning", mro: []Kind{KReturning, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"into", false}}, flags: 0, sqlNames: []string{}},
	KLoadData:                            {name: "LoadData", key: "loaddata", mro: []Kind{KLoadData, KExpression, KExpr}, args: []argSpec{{"this", true}, {"local", false}, {"overwrite", false}, {"temp", false}, {"inpath", false}, {"files", false}, {"partition", false}, {"input_format", false}, {"serde", false}}, flags: 0, sqlNames: []string{}},
	KUpdate:                              {name: "Update", key: "update", mro: []Kind{KUpdate, KExpression, KDML, KExpr}, args: []argSpec{{"with_", false}, {"this", false}, {"expressions", false}, {"from_", false}, {"where", false}, {"returning", false}, {"order", false}, {"limit", false}, {"options", false}, {"hint", false}}, flags: 0, sqlNames: []string{}},
	KMerge:                               {name: "Merge", key: "merge", mro: []Kind{KMerge, KExpression, KDML, KExpr}, args: []argSpec{{"this", true}, {"using", true}, {"on", false}, {"using_cond", false}, {"whens", true}, {"with_", false}, {"returning", false}}, flags: 0, sqlNames: []string{}},
	KWhen:                                {name: "When", key: "when", mro: []Kind{KWhen, KExpression, KExpr}, args: []argSpec{{"matched", true}, {"source", false}, {"condition", false}, {"then", true}}, flags: 0, sqlNames: []string{}},
	KWhens:                               {name: "Whens", key: "whens", mro: []Kind{KWhens, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KCast:                                {name: "Cast", key: "cast", mro: []Kind{KCast, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"to", true}, {"format", false}, {"safe", false}, {"action", false}, {"default", false}}, flags: kfCast, sqlNames: []string{"CAST"}},
	KTryCast:                             {name: "TryCast", key: "trycast", mro: []Kind{KTryCast, KCast, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"to", true}, {"format", false}, {"safe", false}, {"action", false}, {"default", false}, {"requires_string", false}, {"null_on_text_overflow", false}, {"probe_date_format", false}}, flags: kfCast, sqlNames: []string{"TRY_CAST"}},
	KJSONCast:                            {name: "JSONCast", key: "jsoncast", mro: []Kind{KJSONCast, KCast, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"to", true}, {"format", false}, {"safe", false}, {"action", false}, {"default", false}}, flags: kfCast, sqlNames: []string{"J_S_O_N_CAST"}},
	KCastToStrType:                       {name: "CastToStrType", key: "casttostrtype", mro: []Kind{KCastToStrType, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"to", true}}, flags: 0, sqlNames: []string{"CAST_TO_STR_TYPE"}},
	KConvert:                             {name: "Convert", key: "convert", mro: []Kind{KConvert, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"style", false}, {"safe", false}}, flags: 0, sqlNames: []string{"CONVERT"}},
	KIf:                                  {name: "If", key: "if", mro: []Kind{KIf, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"true", true}, {"false", false}}, flags: 0, sqlNames: []string{"IF", "IIF"}},
	KCase:                                {name: "Case", key: "case", mro: []Kind{KCase, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"ifs", true}, {"default", false}}, flags: 0, sqlNames: []string{"CASE"}},
	KCoalesce:                            {name: "Coalesce", key: "coalesce", mro: []Kind{KCoalesce, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"is_nvl", false}, {"is_null", false}}, flags: kfVarLenArgs, sqlNames: []string{"COALESCE", "IFNULL", "NVL"}},
	KDecodeCase:                          {name: "DecodeCase", key: "decodecase", mro: []Kind{KDecodeCase, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"DECODE_CASE"}},
	KEqualNull:                           {name: "EqualNull", key: "equalnull", mro: []Kind{KEqualNull, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"EQUAL_NULL"}},
	KGreatest:                            {name: "Greatest", key: "greatest", mro: []Kind{KGreatest, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"ignore_nulls", true}}, flags: kfVarLenArgs, sqlNames: []string{"GREATEST"}},
	KLeast:                               {name: "Least", key: "least", mro: []Kind{KLeast, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"ignore_nulls", true}}, flags: kfVarLenArgs, sqlNames: []string{"LEAST"}},
	KNullif:                              {name: "Nullif", key: "nullif", mro: []Kind{KNullif, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"NULLIF"}},
	KObjectTransform:                     {name: "ObjectTransform", key: "objecttransform", mro: []Kind{KObjectTransform, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"keep", false}, {"set_", false}}, flags: 0, sqlNames: []string{"OBJECT_TRANSFORM"}},
	KNvl2:                                {name: "Nvl2", key: "nvl2", mro: []Kind{KNvl2, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"true", true}, {"false", false}}, flags: 0, sqlNames: []string{"NVL2"}},
	KTry:                                 {name: "Try", key: "try", mro: []Kind{KTry, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TRY"}},
	KCollate:                             {name: "Collate", key: "collate", mro: []Kind{KCollate, KExpression, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"COLLATE"}},
	KCollation:                           {name: "Collation", key: "collation", mro: []Kind{KCollation, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"COLLATION"}},
	KConnectByRoot:                       {name: "ConnectByRoot", key: "connectbyroot", mro: []Kind{KConnectByRoot, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"CONNECT_BY_ROOT"}},
	KCheckXml:                            {name: "CheckXml", key: "checkxml", mro: []Kind{KCheckXml, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"disable_auto_convert", false}}, flags: 0, sqlNames: []string{"CHECK_XML"}},
	KExists:                              {name: "Exists", key: "exists", mro: []Kind{KExists, KExpression, KFunc, KSubqueryPredicate, KPredicate, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"EXISTS"}},
	KFloat64:                             {name: "Float64", key: "float64", mro: []Kind{KFloat64, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"FLOAT64"}},
	KInt64:                               {name: "Int64", key: "int64", mro: []Kind{KInt64, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"INT64"}},
	KIsArray:                             {name: "IsArray", key: "isarray", mro: []Kind{KIsArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"IS_ARRAY"}},
	KIsNullValue:                         {name: "IsNullValue", key: "isnullvalue", mro: []Kind{KIsNullValue, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"IS_NULL_VALUE"}},
	KLaxBool:                             {name: "LaxBool", key: "laxbool", mro: []Kind{KLaxBool, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"LAX_BOOL"}},
	KLaxFloat64:                          {name: "LaxFloat64", key: "laxfloat64", mro: []Kind{KLaxFloat64, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"LAX_FLOAT64"}},
	KLaxInt64:                            {name: "LaxInt64", key: "laxint64", mro: []Kind{KLaxInt64, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"LAX_INT64"}},
	KLaxString:                           {name: "LaxString", key: "laxstring", mro: []Kind{KLaxString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"LAX_STRING"}},
	KToBoolean:                           {name: "ToBoolean", key: "toboolean", mro: []Kind{KToBoolean, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"safe", false}}, flags: 0, sqlNames: []string{"TO_BOOLEAN"}},
	KToVariant:                           {name: "ToVariant", key: "tovariant", mro: []Kind{KToVariant, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TO_VARIANT"}},
	KCurrentAccount:                      {name: "CurrentAccount", key: "currentaccount", mro: []Kind{KCurrentAccount, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_ACCOUNT"}},
	KCurrentAccountName:                  {name: "CurrentAccountName", key: "currentaccountname", mro: []Kind{KCurrentAccountName, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_ACCOUNT_NAME"}},
	KCurrentAvailableRoles:               {name: "CurrentAvailableRoles", key: "currentavailableroles", mro: []Kind{KCurrentAvailableRoles, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_AVAILABLE_ROLES"}},
	KCurrentCatalog:                      {name: "CurrentCatalog", key: "currentcatalog", mro: []Kind{KCurrentCatalog, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_CATALOG"}},
	KCurrentClient:                       {name: "CurrentClient", key: "currentclient", mro: []Kind{KCurrentClient, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_CLIENT"}},
	KCurrentDatabase:                     {name: "CurrentDatabase", key: "currentdatabase", mro: []Kind{KCurrentDatabase, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_DATABASE"}},
	KCurrentIpAddress:                    {name: "CurrentIpAddress", key: "currentipaddress", mro: []Kind{KCurrentIpAddress, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_IP_ADDRESS"}},
	KCurrentOrganizationName:             {name: "CurrentOrganizationName", key: "currentorganizationname", mro: []Kind{KCurrentOrganizationName, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_ORGANIZATION_NAME"}},
	KCurrentOrganizationUser:             {name: "CurrentOrganizationUser", key: "currentorganizationuser", mro: []Kind{KCurrentOrganizationUser, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_ORGANIZATION_USER"}},
	KCurrentRegion:                       {name: "CurrentRegion", key: "currentregion", mro: []Kind{KCurrentRegion, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_REGION"}},
	KCurrentRole:                         {name: "CurrentRole", key: "currentrole", mro: []Kind{KCurrentRole, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_ROLE"}},
	KCurrentRoleType:                     {name: "CurrentRoleType", key: "currentroletype", mro: []Kind{KCurrentRoleType, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_ROLE_TYPE"}},
	KCurrentSchema:                       {name: "CurrentSchema", key: "currentschema", mro: []Kind{KCurrentSchema, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"CURRENT_SCHEMA"}},
	KCurrentSchemas:                      {name: "CurrentSchemas", key: "currentschemas", mro: []Kind{KCurrentSchemas, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"CURRENT_SCHEMAS"}},
	KCurrentSecondaryRoles:               {name: "CurrentSecondaryRoles", key: "currentsecondaryroles", mro: []Kind{KCurrentSecondaryRoles, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_SECONDARY_ROLES"}},
	KCurrentSession:                      {name: "CurrentSession", key: "currentsession", mro: []Kind{KCurrentSession, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_SESSION"}},
	KCurrentStatement:                    {name: "CurrentStatement", key: "currentstatement", mro: []Kind{KCurrentStatement, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_STATEMENT"}},
	KCurrentTransaction:                  {name: "CurrentTransaction", key: "currenttransaction", mro: []Kind{KCurrentTransaction, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_TRANSACTION"}},
	KCurrentUser:                         {name: "CurrentUser", key: "currentuser", mro: []Kind{KCurrentUser, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"CURRENT_USER"}},
	KCurrentUserId:                       {name: "CurrentUserId", key: "currentuserid", mro: []Kind{KCurrentUserId, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_USER_ID"}},
	KCurrentVersion:                      {name: "CurrentVersion", key: "currentversion", mro: []Kind{KCurrentVersion, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_VERSION"}},
	KCurrentWarehouse:                    {name: "CurrentWarehouse", key: "currentwarehouse", mro: []Kind{KCurrentWarehouse, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_WAREHOUSE"}},
	KSessionUser:                         {name: "SessionUser", key: "sessionuser", mro: []Kind{KSessionUser, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"SESSION_USER"}},
	KAIClassify:                          {name: "AIClassify", key: "aiclassify", mro: []Kind{KAIClassify, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"categories", true}, {"config", false}}, flags: 0, sqlNames: []string{"AI_CLASSIFY"}},
	KAIEmbed:                             {name: "AIEmbed", key: "aiembed", mro: []Kind{KAIEmbed, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"AI_EMBED"}},
	KAISimilarity:                        {name: "AISimilarity", key: "aisimilarity", mro: []Kind{KAISimilarity, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"AI_SIMILARITY"}},
	KAIGenerate:                          {name: "AIGenerate", key: "aigenerate", mro: []Kind{KAIGenerate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"AI_GENERATE"}},
	KFeaturesAtTime:                      {name: "FeaturesAtTime", key: "featuresattime", mro: []Kind{KFeaturesAtTime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"time", false}, {"num_rows", false}, {"ignore_feature_nulls", false}}, flags: 0, sqlNames: []string{"FEATURES_AT_TIME"}},
	KGenerateEmbedding:                   {name: "GenerateEmbedding", key: "generateembedding", mro: []Kind{KGenerateEmbedding, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"params_struct", false}, {"is_text", false}}, flags: 0, sqlNames: []string{"GENERATE_EMBEDDING"}},
	KGenerateText:                        {name: "GenerateText", key: "generatetext", mro: []Kind{KGenerateText, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"params_struct", false}}, flags: 0, sqlNames: []string{"GENERATE_TEXT"}},
	KGenerateTable:                       {name: "GenerateTable", key: "generatetable", mro: []Kind{KGenerateTable, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"params_struct", false}}, flags: 0, sqlNames: []string{"GENERATE_TABLE"}},
	KGenerateBool:                        {name: "GenerateBool", key: "generatebool", mro: []Kind{KGenerateBool, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"params_struct", false}}, flags: 0, sqlNames: []string{"GENERATE_BOOL"}},
	KGenerateInt:                         {name: "GenerateInt", key: "generateint", mro: []Kind{KGenerateInt, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"params_struct", false}}, flags: 0, sqlNames: []string{"GENERATE_INT"}},
	KGenerateDouble:                      {name: "GenerateDouble", key: "generatedouble", mro: []Kind{KGenerateDouble, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"params_struct", false}}, flags: 0, sqlNames: []string{"GENERATE_DOUBLE"}},
	KMLForecast:                          {name: "MLForecast", key: "mlforecast", mro: []Kind{KMLForecast, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"params_struct", false}}, flags: 0, sqlNames: []string{"M_L_FORECAST"}},
	KAIForecast:                          {name: "AIForecast", key: "aiforecast", mro: []Kind{KAIForecast, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"data_col", false}, {"timestamp_col", false}, {"model", false}, {"id_cols", false}, {"horizon", false}, {"forecast_end_timestamp", false}, {"confidence_level", false}, {"output_historical_time_series", false}, {"context_window", false}}, flags: 0, sqlNames: []string{"A_I_FORECAST"}},
	KMLTranslate:                         {name: "MLTranslate", key: "mltranslate", mro: []Kind{KMLTranslate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"params_struct", true}}, flags: 0, sqlNames: []string{"M_L_TRANSLATE"}},
	KPredict:                             {name: "Predict", key: "predict", mro: []Kind{KPredict, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"params_struct", false}}, flags: 0, sqlNames: []string{"PREDICT"}},
	KVectorSearch:                        {name: "VectorSearch", key: "vectorsearch", mro: []Kind{KVectorSearch, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"column_to_search", true}, {"query_table", true}, {"query_column_to_search", false}, {"top_k", false}, {"distance_type", false}, {"options", false}}, flags: 0, sqlNames: []string{"VECTOR_SEARCH"}},
	KReadCSV:                             {name: "ReadCSV", key: "readcsv", mro: []Kind{KReadCSV, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"READ_CSV"}},
	KReadParquet:                         {name: "ReadParquet", key: "readparquet", mro: []Kind{KReadParquet, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"READ_PARQUET"}},
	KXMLElement:                          {name: "XMLElement", key: "xmlelement", mro: []Kind{KXMLElement, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"evalname", false}}, flags: 0, sqlNames: []string{"XMLELEMENT"}},
	KGetIgnoreCase:                       {name: "GetIgnoreCase", key: "getignorecase", mro: []Kind{KGetIgnoreCase, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"GET_IGNORE_CASE"}},
	KXMLGet:                              {name: "XMLGet", key: "xmlget", mro: []Kind{KXMLGet, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"instance", false}}, flags: 0, sqlNames: []string{"XMLGET"}},
	KXMLTable:                            {name: "XMLTable", key: "xmltable", mro: []Kind{KXMLTable, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"namespaces", false}, {"passing", false}, {"columns", false}, {"by_ref", false}}, flags: 0, sqlNames: []string{"X_M_L_TABLE"}},
	KHost:                                {name: "Host", key: "host", mro: []Kind{KHost, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"HOST"}},
	KNetFunc:                             {name: "NetFunc", key: "netfunc", mro: []Kind{KNetFunc, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"NET_FUNC"}},
	KParseIp:                             {name: "ParseIp", key: "parseip", mro: []Kind{KParseIp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"type", true}, {"permissive", false}}, flags: 0, sqlNames: []string{"PARSE_IP"}},
	KRegDomain:                           {name: "RegDomain", key: "regdomain", mro: []Kind{KRegDomain, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"REG_DOMAIN"}},
	KColumns:                             {name: "Columns", key: "columns", mro: []Kind{KColumns, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"unpack", false}}, flags: 0, sqlNames: []string{"COLUMNS"}},
	KNormal:                              {name: "Normal", key: "normal", mro: []Kind{KNormal, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"stddev", true}, {"gen", true}}, flags: 0, sqlNames: []string{"NORMAL"}},
	KRand:                                {name: "Rand", key: "rand", mro: []Kind{KRand, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"lower", false}, {"upper", false}}, flags: 0, sqlNames: []string{"RAND", "RANDOM"}},
	KRandn:                               {name: "Randn", key: "randn", mro: []Kind{KRandn, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"RANDN"}},
	KRandstr:                             {name: "Randstr", key: "randstr", mro: []Kind{KRandstr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"generator", false}}, flags: 0, sqlNames: []string{"RANDSTR"}},
	KRangeBucket:                         {name: "RangeBucket", key: "rangebucket", mro: []Kind{KRangeBucket, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"RANGE_BUCKET"}},
	KRangeN:                              {name: "RangeN", key: "rangen", mro: []Kind{KRangeN, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}, {"each", false}}, flags: 0, sqlNames: []string{"RANGE_N"}},
	KSeq1:                                {name: "Seq1", key: "seq1", mro: []Kind{KSeq1, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"SEQ1"}},
	KSeq2:                                {name: "Seq2", key: "seq2", mro: []Kind{KSeq2, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"SEQ2"}},
	KSeq4:                                {name: "Seq4", key: "seq4", mro: []Kind{KSeq4, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"SEQ4"}},
	KSeq8:                                {name: "Seq8", key: "seq8", mro: []Kind{KSeq8, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"SEQ8"}},
	KUniform:                             {name: "Uniform", key: "uniform", mro: []Kind{KUniform, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"gen", false}, {"seed", false}}, flags: 0, sqlNames: []string{"UNIFORM"}},
	KUuid:                                {name: "Uuid", key: "uuid", mro: []Kind{KUuid, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"name", false}, {"is_string", false}}, flags: 0, sqlNames: []string{"UUID", "GEN_RANDOM_UUID", "GENERATE_UUID", "UUID_STRING"}},
	KWeekStart:                           {name: "WeekStart", key: "weekstart", mro: []Kind{KWeekStart, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"WEEK_START"}},
	KWidthBucket:                         {name: "WidthBucket", key: "widthbucket", mro: []Kind{KWidthBucket, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"min_value", false}, {"max_value", false}, {"num_buckets", false}, {"threshold", false}}, flags: 0, sqlNames: []string{"WIDTH_BUCKET"}},
	KZipf:                                {name: "Zipf", key: "zipf", mro: []Kind{KZipf, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"elementcount", true}, {"gen", true}}, flags: 0, sqlNames: []string{"ZIPF"}},
	KCheckJson:                           {name: "CheckJson", key: "checkjson", mro: []Kind{KCheckJson, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"CHECK_JSON"}},
	KJSONArray:                           {name: "JSONArray", key: "jsonarray", mro: []Kind{KJSONArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}, {"null_handling", false}, {"return_type", false}, {"strict", false}}, flags: kfVarLenArgs, sqlNames: []string{"J_S_O_N_ARRAY"}},
	KJSONArrayAgg:                        {name: "JSONArrayAgg", key: "jsonarrayagg", mro: []Kind{KJSONArrayAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"order", false}, {"null_handling", false}, {"return_type", false}, {"strict", false}}, flags: 0, sqlNames: []string{"J_S_O_N_ARRAY_AGG"}},
	KJSONArrayAppend:                     {name: "JSONArrayAppend", key: "jsonarrayappend", mro: []Kind{KJSONArrayAppend, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"JSON_ARRAY_APPEND"}},
	KJSONArrayContains:                   {name: "JSONArrayContains", key: "jsonarraycontains", mro: []Kind{KJSONArrayContains, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"json_type", false}}, flags: 0, sqlNames: []string{"JSON_ARRAY_CONTAINS"}},
	KJSONArrayInsert:                     {name: "JSONArrayInsert", key: "jsonarrayinsert", mro: []Kind{KJSONArrayInsert, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"JSON_ARRAY_INSERT"}},
	KJSONBContains:                       {name: "JSONBContains", key: "jsonbcontains", mro: []Kind{KJSONBContains, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"JSONB_CONTAINS"}},
	KJSONBContainsAllTopKeys:             {name: "JSONBContainsAllTopKeys", key: "jsonbcontainsalltopkeys", mro: []Kind{KJSONBContainsAllTopKeys, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"J_S_O_N_B_CONTAINS_ALL_TOP_KEYS"}},
	KJSONBContainsAnyTopKeys:             {name: "JSONBContainsAnyTopKeys", key: "jsonbcontainsanytopkeys", mro: []Kind{KJSONBContainsAnyTopKeys, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"J_S_O_N_B_CONTAINS_ANY_TOP_KEYS"}},
	KJSONBDeleteAtPath:                   {name: "JSONBDeleteAtPath", key: "jsonbdeleteatpath", mro: []Kind{KJSONBDeleteAtPath, KExpression, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"J_S_O_N_B_DELETE_AT_PATH"}},
	KJSONBPathExists:                     {name: "JSONBPathExists", key: "jsonbpathexists", mro: []Kind{KJSONBPathExists, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"J_S_O_N_B_PATH_EXISTS"}},
	KJSONBExists:                         {name: "JSONBExists", key: "jsonbexists", mro: []Kind{KJSONBExists, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"path", true}}, flags: 0, sqlNames: []string{"JSONB_EXISTS"}},
	KJSONBExtract:                        {name: "JSONBExtract", key: "jsonbextract", mro: []Kind{KJSONBExtract, KExpression, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"JSONB_EXTRACT"}},
	KJSONBExtractScalar:                  {name: "JSONBExtractScalar", key: "jsonbextractscalar", mro: []Kind{KJSONBExtractScalar, KExpression, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"json_type", false}}, flags: 0, sqlNames: []string{"JSONB_EXTRACT_SCALAR"}},
	KJSONBObjectAgg:                      {name: "JSONBObjectAgg", key: "jsonbobjectagg", mro: []Kind{KJSONBObjectAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"J_S_O_N_B_OBJECT_AGG"}},
	KJSONBool:                            {name: "JSONBool", key: "jsonbool", mro: []Kind{KJSONBool, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"J_S_O_N_BOOL"}},
	KJSONExists:                          {name: "JSONExists", key: "jsonexists", mro: []Kind{KJSONExists, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"path", true}, {"passing", false}, {"on_condition", false}, {"from_dcolonqmark", false}}, flags: 0, sqlNames: []string{"J_S_O_N_EXISTS"}},
	KJSONExtract:                         {name: "JSONExtract", key: "jsonextract", mro: []Kind{KJSONExtract, KExpression, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"only_json_types", false}, {"expressions", false}, {"variant_extract", false}, {"json_query", false}, {"option", false}, {"quote", false}, {"on_condition", false}, {"requires_json", false}, {"emits", false}}, flags: kfVarLenArgs, sqlNames: []string{"JSON_EXTRACT"}},
	KJSONExtractArray:                    {name: "JSONExtractArray", key: "jsonextractarray", mro: []Kind{KJSONExtractArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"JSON_EXTRACT_ARRAY"}},
	KJSONExtractScalar:                   {name: "JSONExtractScalar", key: "jsonextractscalar", mro: []Kind{KJSONExtractScalar, KExpression, KBinary, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"only_json_types", false}, {"expressions", false}, {"json_type", false}, {"scalar_only", false}, {"json_subtype", false}}, flags: kfVarLenArgs, sqlNames: []string{"JSON_EXTRACT_SCALAR"}},
	KJSONFormat:                          {name: "JSONFormat", key: "jsonformat", mro: []Kind{KJSONFormat, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"options", false}, {"is_json", false}, {"to_json", false}}, flags: 0, sqlNames: []string{"JSON_FORMAT"}},
	KJSONKeys:                            {name: "JSONKeys", key: "jsonkeys", mro: []Kind{KJSONKeys, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"JSON_KEYS"}},
	KJSONKeysAtDepth:                     {name: "JSONKeysAtDepth", key: "jsonkeysatdepth", mro: []Kind{KJSONKeysAtDepth, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"mode", false}}, flags: 0, sqlNames: []string{"J_S_O_N_KEYS_AT_DEPTH"}},
	KJSONObject:                          {name: "JSONObject", key: "jsonobject", mro: []Kind{KJSONObject, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}, {"null_handling", false}, {"unique_keys", false}, {"return_type", false}, {"encoding", false}}, flags: 0, sqlNames: []string{"J_S_O_N_OBJECT"}},
	KJSONObjectAgg:                       {name: "JSONObjectAgg", key: "jsonobjectagg", mro: []Kind{KJSONObjectAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}, {"null_handling", false}, {"unique_keys", false}, {"return_type", false}, {"encoding", false}}, flags: 0, sqlNames: []string{"J_S_O_N_OBJECT_AGG"}},
	KJSONRemove:                          {name: "JSONRemove", key: "jsonremove", mro: []Kind{KJSONRemove, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"JSON_REMOVE"}},
	KJSONSet:                             {name: "JSONSet", key: "jsonset", mro: []Kind{KJSONSet, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"JSON_SET"}},
	KJSONStripNulls:                      {name: "JSONStripNulls", key: "jsonstripnulls", mro: []Kind{KJSONStripNulls, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"include_arrays", false}, {"remove_empty", false}}, flags: 0, sqlNames: []string{"JSON_STRIP_NULLS"}},
	KStripNullValue:                      {name: "StripNullValue", key: "stripnullvalue", mro: []Kind{KStripNullValue, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"STRIP_NULL_VALUE"}},
	KJSONTable:                           {name: "JSONTable", key: "jsontable", mro: []Kind{KJSONTable, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"schema", true}, {"path", false}, {"error_handling", false}, {"empty_handling", false}}, flags: 0, sqlNames: []string{"J_S_O_N_TABLE"}},
	KJSONType:                            {name: "JSONType", key: "jsontype", mro: []Kind{KJSONType, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"JSON_TYPE"}},
	KObjectId:                            {name: "ObjectId", key: "objectid", mro: []Kind{KObjectId, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"OBJECT_ID"}},
	KObjectInsert:                        {name: "ObjectInsert", key: "objectinsert", mro: []Kind{KObjectInsert, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"key", true}, {"value", true}, {"update_flag", false}}, flags: 0, sqlNames: []string{"OBJECT_INSERT"}},
	KOpenJSON:                            {name: "OpenJSON", key: "openjson", mro: []Kind{KOpenJSON, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"path", false}, {"expressions", false}}, flags: 0, sqlNames: []string{"OPEN_J_S_O_N"}},
	KParseJSON:                           {name: "ParseJSON", key: "parsejson", mro: []Kind{KParseJSON, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"safe", false}, {"is_literal", false}}, flags: 0, sqlNames: []string{"PARSE_JSON", "JSON_PARSE"}},
	KAcos:                                {name: "Acos", key: "acos", mro: []Kind{KAcos, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ACOS"}},
	KAcosh:                               {name: "Acosh", key: "acosh", mro: []Kind{KAcosh, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ACOSH"}},
	KAsin:                                {name: "Asin", key: "asin", mro: []Kind{KAsin, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ASIN"}},
	KAsinh:                               {name: "Asinh", key: "asinh", mro: []Kind{KAsinh, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ASINH"}},
	KAtan:                                {name: "Atan", key: "atan", mro: []Kind{KAtan, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"ATAN"}},
	KAtanh:                               {name: "Atanh", key: "atanh", mro: []Kind{KAtanh, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ATANH"}},
	KAtan2:                               {name: "Atan2", key: "atan2", mro: []Kind{KAtan2, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"ATAN2"}},
	KCos:                                 {name: "Cos", key: "cos", mro: []Kind{KCos, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"COS"}},
	KCosh:                                {name: "Cosh", key: "cosh", mro: []Kind{KCosh, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"COSH"}},
	KCot:                                 {name: "Cot", key: "cot", mro: []Kind{KCot, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"COT"}},
	KCoth:                                {name: "Coth", key: "coth", mro: []Kind{KCoth, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"COTH"}},
	KCsc:                                 {name: "Csc", key: "csc", mro: []Kind{KCsc, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"CSC"}},
	KCsch:                                {name: "Csch", key: "csch", mro: []Kind{KCsch, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"CSCH"}},
	KDegrees:                             {name: "Degrees", key: "degrees", mro: []Kind{KDegrees, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DEGREES"}},
	KRadians:                             {name: "Radians", key: "radians", mro: []Kind{KRadians, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"RADIANS"}},
	KSec:                                 {name: "Sec", key: "sec", mro: []Kind{KSec, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SEC"}},
	KSech:                                {name: "Sech", key: "sech", mro: []Kind{KSech, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SECH"}},
	KSin:                                 {name: "Sin", key: "sin", mro: []Kind{KSin, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SIN"}},
	KSinh:                                {name: "Sinh", key: "sinh", mro: []Kind{KSinh, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SINH"}},
	KTan:                                 {name: "Tan", key: "tan", mro: []Kind{KTan, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TAN"}},
	KTanh:                                {name: "Tanh", key: "tanh", mro: []Kind{KTanh, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TANH"}},
	KCosineDistance:                      {name: "CosineDistance", key: "cosinedistance", mro: []Kind{KCosineDistance, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"COSINE_DISTANCE"}},
	KDotProduct:                          {name: "DotProduct", key: "dotproduct", mro: []Kind{KDotProduct, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"DOT_PRODUCT"}},
	KEuclideanDistance:                   {name: "EuclideanDistance", key: "euclideandistance", mro: []Kind{KEuclideanDistance, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"EUCLIDEAN_DISTANCE"}},
	KJarowinklerSimilarity:               {name: "JarowinklerSimilarity", key: "jarowinklersimilarity", mro: []Kind{KJarowinklerSimilarity, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"case_insensitive", false}, {"integer_scale", false}}, flags: 0, sqlNames: []string{"JAROWINKLER_SIMILARITY"}},
	KManhattanDistance:                   {name: "ManhattanDistance", key: "manhattandistance", mro: []Kind{KManhattanDistance, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"MANHATTAN_DISTANCE"}},
	KAbs:                                 {name: "Abs", key: "abs", mro: []Kind{KAbs, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ABS"}},
	KCbrt:                                {name: "Cbrt", key: "cbrt", mro: []Kind{KCbrt, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"CBRT"}},
	KCeil:                                {name: "Ceil", key: "ceil", mro: []Kind{KCeil, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"decimals", false}, {"to", false}}, flags: 0, sqlNames: []string{"CEIL", "CEILING"}},
	KExp:                                 {name: "Exp", key: "exp", mro: []Kind{KExp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"EXP"}},
	KFactorial:                           {name: "Factorial", key: "factorial", mro: []Kind{KFactorial, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"FACTORIAL"}},
	KFloor:                               {name: "Floor", key: "floor", mro: []Kind{KFloor, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"decimals", false}, {"to", false}}, flags: 0, sqlNames: []string{"FLOOR"}},
	KIsInf:                               {name: "IsInf", key: "isinf", mro: []Kind{KIsInf, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"IS_INF", "ISINF"}},
	KIsNan:                               {name: "IsNan", key: "isnan", mro: []Kind{KIsNan, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"IS_NAN", "ISNAN"}},
	KLn:                                  {name: "Ln", key: "ln", mro: []Kind{KLn, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"LN"}},
	KLog:                                 {name: "Log", key: "log", mro: []Kind{KLog, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"LOG"}},
	KPi:                                  {name: "Pi", key: "pi", mro: []Kind{KPi, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"PI"}},
	KRint:                                {name: "Rint", key: "rint", mro: []Kind{KRint, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"RINT"}},
	KRound:                               {name: "Round", key: "round", mro: []Kind{KRound, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"decimals", false}, {"truncate", false}, {"casts_non_integer_decimals", false}}, flags: 0, sqlNames: []string{"ROUND"}},
	KSign:                                {name: "Sign", key: "sign", mro: []Kind{KSign, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SIGN", "SIGNUM"}},
	KSqrt:                                {name: "Sqrt", key: "sqrt", mro: []Kind{KSqrt, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SQRT"}},
	KTrunc:                               {name: "Trunc", key: "trunc", mro: []Kind{KTrunc, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"decimals", false}, {"fractions_supported", false}}, flags: 0, sqlNames: []string{"TRUNC", "TRUNCATE"}},
	KSafeAdd:                             {name: "SafeAdd", key: "safeadd", mro: []Kind{KSafeAdd, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"SAFE_ADD"}},
	KSafeDivide:                          {name: "SafeDivide", key: "safedivide", mro: []Kind{KSafeDivide, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"SAFE_DIVIDE"}},
	KSafeMultiply:                        {name: "SafeMultiply", key: "safemultiply", mro: []Kind{KSafeMultiply, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"SAFE_MULTIPLY"}},
	KSafeNegate:                          {name: "SafeNegate", key: "safenegate", mro: []Kind{KSafeNegate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SAFE_NEGATE"}},
	KSafeSubtract:                        {name: "SafeSubtract", key: "safesubtract", mro: []Kind{KSafeSubtract, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"SAFE_SUBTRACT"}},
	KBitwiseAndAgg:                       {name: "BitwiseAndAgg", key: "bitwiseandagg", mro: []Kind{KBitwiseAndAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BITWISE_AND_AGG"}},
	KBitwiseCount:                        {name: "BitwiseCount", key: "bitwisecount", mro: []Kind{KBitwiseCount, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BITWISE_COUNT"}},
	KBitwiseOrAgg:                        {name: "BitwiseOrAgg", key: "bitwiseoragg", mro: []Kind{KBitwiseOrAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BITWISE_OR_AGG"}},
	KBitwiseXorAgg:                       {name: "BitwiseXorAgg", key: "bitwisexoragg", mro: []Kind{KBitwiseXorAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BITWISE_XOR_AGG"}},
	KBitmapBitPosition:                   {name: "BitmapBitPosition", key: "bitmapbitposition", mro: []Kind{KBitmapBitPosition, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BITMAP_BIT_POSITION"}},
	KBitmapBucketNumber:                  {name: "BitmapBucketNumber", key: "bitmapbucketnumber", mro: []Kind{KBitmapBucketNumber, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BITMAP_BUCKET_NUMBER"}},
	KBitmapConstructAgg:                  {name: "BitmapConstructAgg", key: "bitmapconstructagg", mro: []Kind{KBitmapConstructAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BITMAP_CONSTRUCT_AGG"}},
	KBitmapCount:                         {name: "BitmapCount", key: "bitmapcount", mro: []Kind{KBitmapCount, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BITMAP_COUNT"}},
	KBitmapOrAgg:                         {name: "BitmapOrAgg", key: "bitmaporagg", mro: []Kind{KBitmapOrAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BITMAP_OR_AGG"}},
	KBooland:                             {name: "Booland", key: "booland", mro: []Kind{KBooland, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"round_input", false}}, flags: 0, sqlNames: []string{"BOOLAND"}},
	KBoolnot:                             {name: "Boolnot", key: "boolnot", mro: []Kind{KBoolnot, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"round_input", false}}, flags: 0, sqlNames: []string{"BOOLNOT"}},
	KBoolor:                              {name: "Boolor", key: "boolor", mro: []Kind{KBoolor, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"round_input", false}}, flags: 0, sqlNames: []string{"BOOLOR"}},
	KBoolxorAgg:                          {name: "BoolxorAgg", key: "boolxoragg", mro: []Kind{KBoolxorAgg, KExpression, KAggFunc, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BOOLXOR_AGG"}},
	KGetbit:                              {name: "Getbit", key: "getbit", mro: []Kind{KGetbit, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"zero_is_msb", false}}, flags: 0, sqlNames: []string{"GETBIT", "GET_BIT"}},
	KProperty:                            {name: "Property", key: "property", mro: []Kind{KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"value", true}}, flags: 0, sqlNames: []string{}},
	KGrantPrivilege:                      {name: "GrantPrivilege", key: "grantprivilege", mro: []Kind{KGrantPrivilege, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KGrantPrincipal:                      {name: "GrantPrincipal", key: "grantprincipal", mro: []Kind{KGrantPrincipal, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", false}}, flags: 0, sqlNames: []string{}},
	KAllowedValuesProperty:               {name: "AllowedValuesProperty", key: "allowedvaluesproperty", mro: []Kind{KAllowedValuesProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KAlgorithmProperty:                   {name: "AlgorithmProperty", key: "algorithmproperty", mro: []Kind{KAlgorithmProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KApiProperty:                         {name: "ApiProperty", key: "apiproperty", mro: []Kind{KApiProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KApplicationProperty:                 {name: "ApplicationProperty", key: "applicationproperty", mro: []Kind{KApplicationProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KAutoIncrementProperty:               {name: "AutoIncrementProperty", key: "autoincrementproperty", mro: []Kind{KAutoIncrementProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KAutoRefreshProperty:                 {name: "AutoRefreshProperty", key: "autorefreshproperty", mro: []Kind{KAutoRefreshProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KBackupProperty:                      {name: "BackupProperty", key: "backupproperty", mro: []Kind{KBackupProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KBuildProperty:                       {name: "BuildProperty", key: "buildproperty", mro: []Kind{KBuildProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KBlockCompressionProperty:            {name: "BlockCompressionProperty", key: "blockcompressionproperty", mro: []Kind{KBlockCompressionProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"autotemp", false}, {"always", false}, {"default", false}, {"manual", false}, {"never", false}}, flags: 0, sqlNames: []string{}},
	KCalledOnNullInputProperty:           {name: "CalledOnNullInputProperty", key: "calledonnullinputproperty", mro: []Kind{KCalledOnNullInputProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KCatalogProperty:                     {name: "CatalogProperty", key: "catalogproperty", mro: []Kind{KCatalogProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KCharacterSetProperty:                {name: "CharacterSetProperty", key: "charactersetproperty", mro: []Kind{KCharacterSetProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"default", true}}, flags: 0, sqlNames: []string{}},
	KChecksumProperty:                    {name: "ChecksumProperty", key: "checksumproperty", mro: []Kind{KChecksumProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"on", false}, {"default", false}}, flags: 0, sqlNames: []string{}},
	KClusterProperty:                     {name: "ClusterProperty", key: "clusterproperty", mro: []Kind{KClusterProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KCollateProperty:                     {name: "CollateProperty", key: "collateproperty", mro: []Kind{KCollateProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"default", false}}, flags: 0, sqlNames: []string{}},
	KComputeProperty:                     {name: "ComputeProperty", key: "computeproperty", mro: []Kind{KComputeProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KCopyGrantsProperty:                  {name: "CopyGrantsProperty", key: "copygrantsproperty", mro: []Kind{KCopyGrantsProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KDataBlocksizeProperty:               {name: "DataBlocksizeProperty", key: "datablocksizeproperty", mro: []Kind{KDataBlocksizeProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"size", false}, {"units", false}, {"minimum", false}, {"maximum", false}, {"default", false}}, flags: 0, sqlNames: []string{}},
	KDataDeletionProperty:                {name: "DataDeletionProperty", key: "datadeletionproperty", mro: []Kind{KDataDeletionProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"on", true}, {"filter_column", false}, {"retention_period", false}}, flags: 0, sqlNames: []string{}},
	KDatabaseProperty:                    {name: "DatabaseProperty", key: "databaseproperty", mro: []Kind{KDatabaseProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KDefinerProperty:                     {name: "DefinerProperty", key: "definerproperty", mro: []Kind{KDefinerProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KDistKeyProperty:                     {name: "DistKeyProperty", key: "distkeyproperty", mro: []Kind{KDistKeyProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KDistributedByProperty:               {name: "DistributedByProperty", key: "distributedbyproperty", mro: []Kind{KDistributedByProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", false}, {"kind", true}, {"buckets", false}, {"order", false}}, flags: 0, sqlNames: []string{}},
	KDistStyleProperty:                   {name: "DistStyleProperty", key: "diststyleproperty", mro: []Kind{KDistStyleProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KDuplicateKeyProperty:                {name: "DuplicateKeyProperty", key: "duplicatekeyproperty", mro: []Kind{KDuplicateKeyProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KEngineProperty:                      {name: "EngineProperty", key: "engineproperty", mro: []Kind{KEngineProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KUuidProperty:                        {name: "UuidProperty", key: "uuidproperty", mro: []Kind{KUuidProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KHeapProperty:                        {name: "HeapProperty", key: "heapproperty", mro: []Kind{KHeapProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KHybridProperty:                      {name: "HybridProperty", key: "hybridproperty", mro: []Kind{KHybridProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KHandlerProperty:                     {name: "HandlerProperty", key: "handlerproperty", mro: []Kind{KHandlerProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KParameterStyleProperty:              {name: "ParameterStyleProperty", key: "parameterstyleproperty", mro: []Kind{KParameterStyleProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KToTableProperty:                     {name: "ToTableProperty", key: "totableproperty", mro: []Kind{KToTableProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KExecuteAsProperty:                   {name: "ExecuteAsProperty", key: "executeasproperty", mro: []Kind{KExecuteAsProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KExternalProperty:                    {name: "ExternalProperty", key: "externalproperty", mro: []Kind{KExternalProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KFallbackProperty:                    {name: "FallbackProperty", key: "fallbackproperty", mro: []Kind{KFallbackProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"no", true}, {"protection", false}}, flags: 0, sqlNames: []string{}},
	KFileFormatProperty:                  {name: "FileFormatProperty", key: "fileformatproperty", mro: []Kind{KFileFormatProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}, {"hive_format", false}}, flags: 0, sqlNames: []string{}},
	KCredentialsProperty:                 {name: "CredentialsProperty", key: "credentialsproperty", mro: []Kind{KCredentialsProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KFreespaceProperty:                   {name: "FreespaceProperty", key: "freespaceproperty", mro: []Kind{KFreespaceProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"percent", false}}, flags: 0, sqlNames: []string{}},
	KGlobalProperty:                      {name: "GlobalProperty", key: "globalproperty", mro: []Kind{KGlobalProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KIcebergProperty:                     {name: "IcebergProperty", key: "icebergproperty", mro: []Kind{KIcebergProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KInheritsProperty:                    {name: "InheritsProperty", key: "inheritsproperty", mro: []Kind{KInheritsProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KInputModelProperty:                  {name: "InputModelProperty", key: "inputmodelproperty", mro: []Kind{KInputModelProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KOutputModelProperty:                 {name: "OutputModelProperty", key: "outputmodelproperty", mro: []Kind{KOutputModelProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KIsolatedLoadingProperty:             {name: "IsolatedLoadingProperty", key: "isolatedloadingproperty", mro: []Kind{KIsolatedLoadingProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"no", false}, {"concurrent", false}, {"target", false}}, flags: 0, sqlNames: []string{}},
	KJournalProperty:                     {name: "JournalProperty", key: "journalproperty", mro: []Kind{KJournalProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"no", false}, {"dual", false}, {"before", false}, {"local", false}, {"after", false}}, flags: 0, sqlNames: []string{}},
	KLanguageProperty:                    {name: "LanguageProperty", key: "languageproperty", mro: []Kind{KLanguageProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KEnviromentProperty:                  {name: "EnviromentProperty", key: "enviromentproperty", mro: []Kind{KEnviromentProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KClusteredByProperty:                 {name: "ClusteredByProperty", key: "clusteredbyproperty", mro: []Kind{KClusteredByProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"sorted_by", false}, {"buckets", true}}, flags: 0, sqlNames: []string{}},
	KDictProperty:                        {name: "DictProperty", key: "dictproperty", mro: []Kind{KDictProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", true}, {"settings", false}}, flags: 0, sqlNames: []string{}},
	KDictSubProperty:                     {name: "DictSubProperty", key: "dictsubproperty", mro: []Kind{KDictSubProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"value", true}}, flags: 0, sqlNames: []string{}},
	KDictRange:                           {name: "DictRange", key: "dictrange", mro: []Kind{KDictRange, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"min", true}, {"max", true}}, flags: 0, sqlNames: []string{}},
	KDynamicProperty:                     {name: "DynamicProperty", key: "dynamicproperty", mro: []Kind{KDynamicProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KOnCluster:                           {name: "OnCluster", key: "oncluster", mro: []Kind{KOnCluster, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KEmptyProperty:                       {name: "EmptyProperty", key: "emptyproperty", mro: []Kind{KEmptyProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KLikeProperty:                        {name: "LikeProperty", key: "likeproperty", mro: []Kind{KLikeProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KLocationProperty:                    {name: "LocationProperty", key: "locationproperty", mro: []Kind{KLocationProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KLockProperty:                        {name: "LockProperty", key: "lockproperty", mro: []Kind{KLockProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KLockingProperty:                     {name: "LockingProperty", key: "lockingproperty", mro: []Kind{KLockingProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}, {"kind", true}, {"for_or_in", false}, {"lock_type", true}, {"override", false}}, flags: 0, sqlNames: []string{}},
	KLogProperty:                         {name: "LogProperty", key: "logproperty", mro: []Kind{KLogProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"no", true}}, flags: 0, sqlNames: []string{}},
	KMaskingProperty:                     {name: "MaskingProperty", key: "maskingproperty", mro: []Kind{KMaskingProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KMaterializedProperty:                {name: "MaterializedProperty", key: "materializedproperty", mro: []Kind{KMaterializedProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KMergeBlockRatioProperty:             {name: "MergeBlockRatioProperty", key: "mergeblockratioproperty", mro: []Kind{KMergeBlockRatioProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}, {"no", false}, {"default", false}, {"percent", false}}, flags: 0, sqlNames: []string{}},
	KModuleProperty:                      {name: "ModuleProperty", key: "moduleproperty", mro: []Kind{KModuleProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KNetworkProperty:                     {name: "NetworkProperty", key: "networkproperty", mro: []Kind{KNetworkProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KNoPrimaryIndexProperty:              {name: "NoPrimaryIndexProperty", key: "noprimaryindexproperty", mro: []Kind{KNoPrimaryIndexProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KOnProperty:                          {name: "OnProperty", key: "onproperty", mro: []Kind{KOnProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KOnCommitProperty:                    {name: "OnCommitProperty", key: "oncommitproperty", mro: []Kind{KOnCommitProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"delete", false}}, flags: 0, sqlNames: []string{}},
	KPartitionedByProperty:               {name: "PartitionedByProperty", key: "partitionedbyproperty", mro: []Kind{KPartitionedByProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KPartitionedByBucket:                 {name: "PartitionedByBucket", key: "partitionedbybucket", mro: []Kind{KPartitionedByBucket, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KPartitionByTruncate:                 {name: "PartitionByTruncate", key: "partitionbytruncate", mro: []Kind{KPartitionByTruncate, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KPartitionByRangeProperty:            {name: "PartitionByRangeProperty", key: "partitionbyrangeproperty", mro: []Kind{KPartitionByRangeProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"partition_expressions", true}, {"create_expressions", true}}, flags: 0, sqlNames: []string{}},
	KPartitionByRangePropertyDynamic:     {name: "PartitionByRangePropertyDynamic", key: "partitionbyrangepropertydynamic", mro: []Kind{KPartitionByRangePropertyDynamic, KExpression, KExpr}, args: []argSpec{{"this", false}, {"start", true}, {"end", true}, {"every", true}}, flags: 0, sqlNames: []string{}},
	KRollupProperty:                      {name: "RollupProperty", key: "rollupproperty", mro: []Kind{KRollupProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KRollupIndex:                         {name: "RollupIndex", key: "rollupindex", mro: []Kind{KRollupIndex, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}, {"from_index", false}, {"properties", false}}, flags: 0, sqlNames: []string{}},
	KRowAccessProperty:                   {name: "RowAccessProperty", key: "rowaccessproperty", mro: []Kind{KRowAccessProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KPartitionByListProperty:             {name: "PartitionByListProperty", key: "partitionbylistproperty", mro: []Kind{KPartitionByListProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"partition_expressions", true}, {"create_expressions", true}}, flags: 0, sqlNames: []string{}},
	KPartitionList:                       {name: "PartitionList", key: "partitionlist", mro: []Kind{KPartitionList, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: 0, sqlNames: []string{}},
	KRefreshTriggerProperty:              {name: "RefreshTriggerProperty", key: "refreshtriggerproperty", mro: []Kind{KRefreshTriggerProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"method", false}, {"kind", false}, {"every", false}, {"unit", false}, {"starts", false}}, flags: 0, sqlNames: []string{}},
	KUniqueKeyProperty:                   {name: "UniqueKeyProperty", key: "uniquekeyproperty", mro: []Kind{KUniqueKeyProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KPartitionBoundSpec:                  {name: "PartitionBoundSpec", key: "partitionboundspec", mro: []Kind{KPartitionBoundSpec, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expression", false}, {"from_expressions", false}, {"to_expressions", false}}, flags: 0, sqlNames: []string{}},
	KPartitionedOfProperty:               {name: "PartitionedOfProperty", key: "partitionedofproperty", mro: []Kind{KPartitionedOfProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KStreamingTableProperty:              {name: "StreamingTableProperty", key: "streamingtableproperty", mro: []Kind{KStreamingTableProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KRemoteWithConnectionModelProperty:   {name: "RemoteWithConnectionModelProperty", key: "remotewithconnectionmodelproperty", mro: []Kind{KRemoteWithConnectionModelProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KReturnsProperty:                     {name: "ReturnsProperty", key: "returnsproperty", mro: []Kind{KReturnsProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}, {"is_table", false}, {"table", false}, {"null", false}}, flags: 0, sqlNames: []string{}},
	KStrictProperty:                      {name: "StrictProperty", key: "strictproperty", mro: []Kind{KStrictProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KRowFormatProperty:                   {name: "RowFormatProperty", key: "rowformatproperty", mro: []Kind{KRowFormatProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KRowFormatDelimitedProperty:          {name: "RowFormatDelimitedProperty", key: "rowformatdelimitedproperty", mro: []Kind{KRowFormatDelimitedProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"fields", false}, {"escaped", false}, {"collection_items", false}, {"map_keys", false}, {"lines", false}, {"null", false}, {"serde", false}}, flags: 0, sqlNames: []string{}},
	KRowFormatSerdeProperty:              {name: "RowFormatSerdeProperty", key: "rowformatserdeproperty", mro: []Kind{KRowFormatSerdeProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"serde_properties", false}}, flags: 0, sqlNames: []string{}},
	KQueryTransform:                      {name: "QueryTransform", key: "querytransform", mro: []Kind{KQueryTransform, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"command_script", true}, {"schema", false}, {"row_format_before", false}, {"record_writer", false}, {"row_format_after", false}, {"record_reader", false}}, flags: 0, sqlNames: []string{}},
	KSampleProperty:                      {name: "SampleProperty", key: "sampleproperty", mro: []Kind{KSampleProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KSchemaCommentProperty:               {name: "SchemaCommentProperty", key: "schemacommentproperty", mro: []Kind{KSchemaCommentProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KSemanticView:                        {name: "SemanticView", key: "semanticview", mro: []Kind{KSemanticView, KExpression, KExpr}, args: []argSpec{{"this", true}, {"metrics", false}, {"dimensions", false}, {"facts", false}, {"where", false}}, flags: 0, sqlNames: []string{}},
	KSerdeProperties:                     {name: "SerdeProperties", key: "serdeproperties", mro: []Kind{KSerdeProperties, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"with_", false}}, flags: 0, sqlNames: []string{}},
	KSetProperty:                         {name: "SetProperty", key: "setproperty", mro: []Kind{KSetProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"multi", true}}, flags: 0, sqlNames: []string{}},
	KSharingProperty:                     {name: "SharingProperty", key: "sharingproperty", mro: []Kind{KSharingProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KSetConfigProperty:                   {name: "SetConfigProperty", key: "setconfigproperty", mro: []Kind{KSetConfigProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KSettingsProperty:                    {name: "SettingsProperty", key: "settingsproperty", mro: []Kind{KSettingsProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KSortKeyProperty:                     {name: "SortKeyProperty", key: "sortkeyproperty", mro: []Kind{KSortKeyProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"compound", false}}, flags: 0, sqlNames: []string{}},
	KSqlReadWriteProperty:                {name: "SqlReadWriteProperty", key: "sqlreadwriteproperty", mro: []Kind{KSqlReadWriteProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KSqlSecurityProperty:                 {name: "SqlSecurityProperty", key: "sqlsecurityproperty", mro: []Kind{KSqlSecurityProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KStabilityProperty:                   {name: "StabilityProperty", key: "stabilityproperty", mro: []Kind{KStabilityProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KStorageHandlerProperty:              {name: "StorageHandlerProperty", key: "storagehandlerproperty", mro: []Kind{KStorageHandlerProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KUsingProperty:                       {name: "UsingProperty", key: "usingproperty", mro: []Kind{KUsingProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", true}}, flags: 0, sqlNames: []string{}},
	KTemporaryProperty:                   {name: "TemporaryProperty", key: "temporaryproperty", mro: []Kind{KTemporaryProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KVirtualProperty:                     {name: "VirtualProperty", key: "virtualproperty", mro: []Kind{KVirtualProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KSecureProperty:                      {name: "SecureProperty", key: "secureproperty", mro: []Kind{KSecureProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KSecurityIntegrationProperty:         {name: "SecurityIntegrationProperty", key: "securityintegrationproperty", mro: []Kind{KSecurityIntegrationProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KTags:                                {name: "Tags", key: "tags", mro: []Kind{KTags, KProperty, KExpression, KColumnConstraintKind, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KTransformModelProperty:              {name: "TransformModelProperty", key: "transformmodelproperty", mro: []Kind{KTransformModelProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KTransientProperty:                   {name: "TransientProperty", key: "transientproperty", mro: []Kind{KTransientProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KUnloggedProperty:                    {name: "UnloggedProperty", key: "unloggedproperty", mro: []Kind{KUnloggedProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KUsingTemplateProperty:               {name: "UsingTemplateProperty", key: "usingtemplateproperty", mro: []Kind{KUsingTemplateProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KViewAttributeProperty:               {name: "ViewAttributeProperty", key: "viewattributeproperty", mro: []Kind{KViewAttributeProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KVolatileProperty:                    {name: "VolatileProperty", key: "volatileproperty", mro: []Kind{KVolatileProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KWithDataProperty:                    {name: "WithDataProperty", key: "withdataproperty", mro: []Kind{KWithDataProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"no", true}, {"statistics", false}}, flags: 0, sqlNames: []string{}},
	KWithJournalTableProperty:            {name: "WithJournalTableProperty", key: "withjournaltableproperty", mro: []Kind{KWithJournalTableProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KWithSchemaBindingProperty:           {name: "WithSchemaBindingProperty", key: "withschemabindingproperty", mro: []Kind{KWithSchemaBindingProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KWithSystemVersioningProperty:        {name: "WithSystemVersioningProperty", key: "withsystemversioningproperty", mro: []Kind{KWithSystemVersioningProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"on", false}, {"this", false}, {"data_consistency", false}, {"retention_period", false}, {"with_", true}}, flags: 0, sqlNames: []string{}},
	KWithProcedureOptions:                {name: "WithProcedureOptions", key: "withprocedureoptions", mro: []Kind{KWithProcedureOptions, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KEncodeProperty:                      {name: "EncodeProperty", key: "encodeproperty", mro: []Kind{KEncodeProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"properties", false}, {"key", false}}, flags: 0, sqlNames: []string{}},
	KIncludeProperty:                     {name: "IncludeProperty", key: "includeproperty", mro: []Kind{KIncludeProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"alias", false}, {"column_def", false}}, flags: 0, sqlNames: []string{}},
	KForceProperty:                       {name: "ForceProperty", key: "forceproperty", mro: []Kind{KForceProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KProperties:                          {name: "Properties", key: "properties", mro: []Kind{KProperties, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KSelectable:                          {name: "Selectable", key: "selectable", mro: []Kind{KSelectable, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KDerivedTable:                        {name: "DerivedTable", key: "derivedtable", mro: []Kind{KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KUDTF:                                {name: "UDTF", key: "udtf", mro: []Kind{KUDTF, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KQuery:                               {name: "Query", key: "query", mro: []Kind{KQuery, KSelectable, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KQueryBand:                           {name: "QueryBand", key: "queryband", mro: []Kind{KQueryBand, KExpression, KExpr}, args: []argSpec{{"this", true}, {"scope", false}, {"update", false}}, flags: 0, sqlNames: []string{}},
	KRecursiveWithSearch:                 {name: "RecursiveWithSearch", key: "recursivewithsearch", mro: []Kind{KRecursiveWithSearch, KExpression, KExpr}, args: []argSpec{{"kind", true}, {"this", true}, {"expression", true}, {"to", false}, {"default", false}, {"using", false}}, flags: 0, sqlNames: []string{}},
	KWith:                                {name: "With", key: "with", mro: []Kind{KWith, KExpression, KExpr}, args: []argSpec{{"expressions", false}, {"recursive", false}, {"search", false}, {"cycle", false}, {"udfs", false}}, flags: 0, sqlNames: []string{}},
	KCTE:                                 {name: "CTE", key: "cte", mro: []Kind{KCTE, KExpression, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"this", true}, {"alias", true}, {"scalar", false}, {"materialized", false}, {"key_expressions", false}}, flags: 0, sqlNames: []string{}},
	KProjectionDef:                       {name: "ProjectionDef", key: "projectiondef", mro: []Kind{KProjectionDef, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KTableAlias:                          {name: "TableAlias", key: "tablealias", mro: []Kind{KTableAlias, KExpression, KExpr}, args: []argSpec{{"this", false}, {"columns", false}}, flags: 0, sqlNames: []string{}},
	KBitString:                           {name: "BitString", key: "bitstring", mro: []Kind{KBitString, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: kfPrimitive, sqlNames: []string{}},
	KHexString:                           {name: "HexString", key: "hexstring", mro: []Kind{KHexString, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"is_integer", false}}, flags: kfPrimitive, sqlNames: []string{}},
	KByteString:                          {name: "ByteString", key: "bytestring", mro: []Kind{KByteString, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"is_bytes", false}}, flags: kfPrimitive, sqlNames: []string{}},
	KRawString:                           {name: "RawString", key: "rawstring", mro: []Kind{KRawString, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: kfPrimitive, sqlNames: []string{}},
	KUnicodeString:                       {name: "UnicodeString", key: "unicodestring", mro: []Kind{KUnicodeString, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"escape", false}}, flags: 0, sqlNames: []string{}},
	KColumnPosition:                      {name: "ColumnPosition", key: "columnposition", mro: []Kind{KColumnPosition, KExpression, KExpr}, args: []argSpec{{"this", false}, {"position", true}}, flags: 0, sqlNames: []string{}},
	KColumnDef:                           {name: "ColumnDef", key: "columndef", mro: []Kind{KColumnDef, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", false}, {"constraints", false}, {"exists", false}, {"position", false}, {"default", false}, {"output", false}}, flags: 0, sqlNames: []string{}},
	KChanges:                             {name: "Changes", key: "changes", mro: []Kind{KChanges, KExpression, KExpr}, args: []argSpec{{"information", true}, {"at_before", false}, {"end", false}}, flags: 0, sqlNames: []string{}},
	KConnect:                             {name: "Connect", key: "connect", mro: []Kind{KConnect, KExpression, KExpr}, args: []argSpec{{"start", false}, {"connect", true}, {"nocycle", false}}, flags: 0, sqlNames: []string{}},
	KPrior:                               {name: "Prior", key: "prior", mro: []Kind{KPrior, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KInto:                                {name: "Into", key: "into", mro: []Kind{KInto, KExpression, KExpr}, args: []argSpec{{"this", false}, {"temporary", false}, {"unlogged", false}, {"bulk_collect", false}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KFrom:                                {name: "From", key: "from", mro: []Kind{KFrom, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KHaving:                              {name: "Having", key: "having", mro: []Kind{KHaving, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KIndex:                               {name: "Index", key: "index", mro: []Kind{KIndex, KExpression, KExpr}, args: []argSpec{{"this", false}, {"table", false}, {"unique", false}, {"primary", false}, {"amp", false}, {"params", false}}, flags: 0, sqlNames: []string{}},
	KConditionalInsert:                   {name: "ConditionalInsert", key: "conditionalinsert", mro: []Kind{KConditionalInsert, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"else_", false}}, flags: 0, sqlNames: []string{}},
	KMultitableInserts:                   {name: "MultitableInserts", key: "multitableinserts", mro: []Kind{KMultitableInserts, KExpression, KExpr}, args: []argSpec{{"with_", false}, {"expressions", true}, {"kind", false}, {"source", true}}, flags: 0, sqlNames: []string{}},
	KOnCondition:                         {name: "OnCondition", key: "oncondition", mro: []Kind{KOnCondition, KExpression, KExpr}, args: []argSpec{{"error", false}, {"empty", false}, {"null", false}}, flags: 0, sqlNames: []string{}},
	KIntroducer:                          {name: "Introducer", key: "introducer", mro: []Kind{KIntroducer, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KNational:                            {name: "National", key: "national", mro: []Kind{KNational, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: kfPrimitive, sqlNames: []string{}},
	KPartition:                           {name: "Partition", key: "partition", mro: []Kind{KPartition, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"subpartition", false}}, flags: 0, sqlNames: []string{}},
	KPartitionRange:                      {name: "PartitionRange", key: "partitionrange", mro: []Kind{KPartitionRange, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KPartitionId:                         {name: "PartitionId", key: "partitionid", mro: []Kind{KPartitionId, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KFetch:                               {name: "Fetch", key: "fetch", mro: []Kind{KFetch, KExpression, KExpr}, args: []argSpec{{"direction", false}, {"count", false}, {"limit_options", false}}, flags: 0, sqlNames: []string{}},
	KGrant:                               {name: "Grant", key: "grant", mro: []Kind{KGrant, KExpression, KExpr}, args: []argSpec{{"privileges", true}, {"kind", false}, {"securable", true}, {"principals", true}, {"grant_option", false}}, flags: 0, sqlNames: []string{}},
	KRevoke:                              {name: "Revoke", key: "revoke", mro: []Kind{KRevoke, KExpression, KExpr}, args: []argSpec{{"privileges", true}, {"kind", false}, {"securable", true}, {"principals", true}, {"grant_option", false}, {"cascade", false}}, flags: 0, sqlNames: []string{}},
	KGroup:                               {name: "Group", key: "group", mro: []Kind{KGroup, KExpression, KExpr}, args: []argSpec{{"expressions", false}, {"grouping_sets", false}, {"cube", false}, {"rollup", false}, {"totals", false}, {"all", false}}, flags: 0, sqlNames: []string{}},
	KCube:                                {name: "Cube", key: "cube", mro: []Kind{KCube, KExpression, KExpr}, args: []argSpec{{"expressions", false}}, flags: 0, sqlNames: []string{}},
	KRollup:                              {name: "Rollup", key: "rollup", mro: []Kind{KRollup, KExpression, KExpr}, args: []argSpec{{"expressions", false}}, flags: 0, sqlNames: []string{}},
	KGroupingSets:                        {name: "GroupingSets", key: "groupingsets", mro: []Kind{KGroupingSets, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KLambda:                              {name: "Lambda", key: "lambda", mro: []Kind{KLambda, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}, {"colon", false}}, flags: 0, sqlNames: []string{}},
	KLimit:                               {name: "Limit", key: "limit", mro: []Kind{KLimit, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expression", true}, {"offset", false}, {"limit_options", false}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KLimitOptions:                        {name: "LimitOptions", key: "limitoptions", mro: []Kind{KLimitOptions, KExpression, KExpr}, args: []argSpec{{"percent", false}, {"rows", false}, {"with_ties", false}}, flags: 0, sqlNames: []string{}},
	KJoin:                                {name: "Join", key: "join", mro: []Kind{KJoin, KExpression, KExpr}, args: []argSpec{{"this", true}, {"on", false}, {"side", false}, {"kind", false}, {"using", false}, {"method", false}, {"global_", false}, {"hint", false}, {"match_condition", false}, {"nearest", false}, {"directed", false}, {"expressions", false}, {"pivots", false}}, flags: 0, sqlNames: []string{}},
	KLateral:                             {name: "Lateral", key: "lateral", mro: []Kind{KLateral, KExpression, KUDTF, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"this", true}, {"view", false}, {"outer", false}, {"alias", false}, {"cross_apply", false}, {"ordinality", false}}, flags: 0, sqlNames: []string{}},
	KTableFromRows:                       {name: "TableFromRows", key: "tablefromrows", mro: []Kind{KTableFromRows, KExpression, KUDTF, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"this", true}, {"alias", false}, {"joins", false}, {"pivots", false}, {"sample", false}}, flags: 0, sqlNames: []string{}},
	KMatchRecognizeMeasure:               {name: "MatchRecognizeMeasure", key: "matchrecognizemeasure", mro: []Kind{KMatchRecognizeMeasure, KExpression, KExpr}, args: []argSpec{{"this", true}, {"window_frame", false}}, flags: 0, sqlNames: []string{}},
	KMatchRecognize:                      {name: "MatchRecognize", key: "matchrecognize", mro: []Kind{KMatchRecognize, KExpression, KExpr}, args: []argSpec{{"partition_by", false}, {"order", false}, {"measures", false}, {"rows", false}, {"after", false}, {"pattern", false}, {"define", false}, {"alias", false}}, flags: 0, sqlNames: []string{}},
	KFinal:                               {name: "Final", key: "final", mro: []Kind{KFinal, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KOffset:                              {name: "Offset", key: "offset", mro: []Kind{KOffset, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expression", true}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KOrder:                               {name: "Order", key: "order", mro: []Kind{KOrder, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", true}, {"siblings", false}}, flags: 0, sqlNames: []string{}},
	KWithFill:                            {name: "WithFill", key: "withfill", mro: []Kind{KWithFill, KExpression, KExpr}, args: []argSpec{{"from_", false}, {"to", false}, {"step", false}, {"interpolate", false}}, flags: 0, sqlNames: []string{}},
	KSkipJSONColumn:                      {name: "SkipJSONColumn", key: "skipjsoncolumn", mro: []Kind{KSkipJSONColumn, KExpression, KExpr}, args: []argSpec{{"regexp", false}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KCluster:                             {name: "Cluster", key: "cluster", mro: []Kind{KCluster, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KDistribute:                          {name: "Distribute", key: "distribute", mro: []Kind{KDistribute, KOrder, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", true}, {"siblings", false}}, flags: 0, sqlNames: []string{}},
	KSort:                                {name: "Sort", key: "sort", mro: []Kind{KSort, KOrder, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", true}, {"siblings", false}}, flags: 0, sqlNames: []string{}},
	KQualify:                             {name: "Qualify", key: "qualify", mro: []Kind{KQualify, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KInputOutputFormat:                   {name: "InputOutputFormat", key: "inputoutputformat", mro: []Kind{KInputOutputFormat, KExpression, KExpr}, args: []argSpec{{"input_format", false}, {"output_format", false}}, flags: 0, sqlNames: []string{}},
	KReturn:                              {name: "Return", key: "return", mro: []Kind{KReturn, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KTuple:                               {name: "Tuple", key: "tuple", mro: []Kind{KTuple, KExpression, KExpr}, args: []argSpec{{"expressions", false}}, flags: 0, sqlNames: []string{}},
	KQueryOption:                         {name: "QueryOption", key: "queryoption", mro: []Kind{KQueryOption, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KForClause:                           {name: "ForClause", key: "forclause", mro: []Kind{KForClause, KExpression, KExpr}, args: []argSpec{{"kind", true}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KWithTableHint:                       {name: "WithTableHint", key: "withtablehint", mro: []Kind{KWithTableHint, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KIndexTableHint:                      {name: "IndexTableHint", key: "indextablehint", mro: []Kind{KIndexTableHint, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"target", false}}, flags: 0, sqlNames: []string{}},
	KHistoricalData:                      {name: "HistoricalData", key: "historicaldata", mro: []Kind{KHistoricalData, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KPut:                                 {name: "Put", key: "put", mro: []Kind{KPut, KExpression, KExpr}, args: []argSpec{{"this", true}, {"target", true}, {"properties", false}}, flags: 0, sqlNames: []string{}},
	KGet:                                 {name: "Get", key: "get", mro: []Kind{KGet, KExpression, KExpr}, args: []argSpec{{"this", true}, {"target", true}, {"properties", false}}, flags: 0, sqlNames: []string{}},
	KTable:                               {name: "Table", key: "table", mro: []Kind{KTable, KExpression, KSelectable, KExpr}, args: []argSpec{{"this", false}, {"alias", false}, {"db", false}, {"catalog", false}, {"laterals", false}, {"joins", false}, {"pivots", false}, {"hints", false}, {"system_time", false}, {"version", false}, {"format", false}, {"pattern", false}, {"ordinality", false}, {"when", false}, {"only", false}, {"partition", false}, {"changes", false}, {"rows_from", false}, {"sample", false}, {"indexed", false}}, flags: 0, sqlNames: []string{}},
	KSetOperation:                        {name: "SetOperation", key: "setoperation", mro: []Kind{KSetOperation, KExpression, KQuery, KSelectable, KExpr}, args: []argSpec{{"with_", false}, {"this", true}, {"expression", true}, {"distinct", false}, {"by_name", false}, {"side", false}, {"kind", false}, {"on", false}, {"match", false}, {"laterals", false}, {"joins", false}, {"connect", false}, {"pivots", false}, {"prewhere", false}, {"where", false}, {"group", false}, {"having", false}, {"qualify", false}, {"windows", false}, {"distribute", false}, {"sort", false}, {"cluster", false}, {"order", false}, {"limit", false}, {"offset", false}, {"locks", false}, {"sample", false}, {"settings", false}, {"format", false}, {"options", false}, {"for_", false}}, flags: 0, sqlNames: []string{}},
	KUnion:                               {name: "Union", key: "union", mro: []Kind{KUnion, KSetOperation, KExpression, KQuery, KSelectable, KExpr}, args: []argSpec{{"with_", false}, {"this", true}, {"expression", true}, {"distinct", false}, {"by_name", false}, {"side", false}, {"kind", false}, {"on", false}, {"match", false}, {"laterals", false}, {"joins", false}, {"connect", false}, {"pivots", false}, {"prewhere", false}, {"where", false}, {"group", false}, {"having", false}, {"qualify", false}, {"windows", false}, {"distribute", false}, {"sort", false}, {"cluster", false}, {"order", false}, {"limit", false}, {"offset", false}, {"locks", false}, {"sample", false}, {"settings", false}, {"format", false}, {"options", false}, {"for_", false}}, flags: 0, sqlNames: []string{}},
	KExcept:                              {name: "Except", key: "except", mro: []Kind{KExcept, KSetOperation, KExpression, KQuery, KSelectable, KExpr}, args: []argSpec{{"with_", false}, {"this", true}, {"expression", true}, {"distinct", false}, {"by_name", false}, {"side", false}, {"kind", false}, {"on", false}, {"match", false}, {"laterals", false}, {"joins", false}, {"connect", false}, {"pivots", false}, {"prewhere", false}, {"where", false}, {"group", false}, {"having", false}, {"qualify", false}, {"windows", false}, {"distribute", false}, {"sort", false}, {"cluster", false}, {"order", false}, {"limit", false}, {"offset", false}, {"locks", false}, {"sample", false}, {"settings", false}, {"format", false}, {"options", false}, {"for_", false}}, flags: 0, sqlNames: []string{}},
	KIntersect:                           {name: "Intersect", key: "intersect", mro: []Kind{KIntersect, KSetOperation, KExpression, KQuery, KSelectable, KExpr}, args: []argSpec{{"with_", false}, {"this", true}, {"expression", true}, {"distinct", false}, {"by_name", false}, {"side", false}, {"kind", false}, {"on", false}, {"match", false}, {"laterals", false}, {"joins", false}, {"connect", false}, {"pivots", false}, {"prewhere", false}, {"where", false}, {"group", false}, {"having", false}, {"qualify", false}, {"windows", false}, {"distribute", false}, {"sort", false}, {"cluster", false}, {"order", false}, {"limit", false}, {"offset", false}, {"locks", false}, {"sample", false}, {"settings", false}, {"format", false}, {"options", false}, {"for_", false}}, flags: 0, sqlNames: []string{}},
	KValues:                              {name: "Values", key: "values", mro: []Kind{KValues, KExpression, KUDTF, KDerivedTable, KSelectable, KExpr}, args: []argSpec{{"expressions", true}, {"alias", false}, {"order", false}, {"limit", false}, {"offset", false}, {"pivots", false}}, flags: 0, sqlNames: []string{}},
	KVersion:                             {name: "Version", key: "version", mro: []Kind{KVersion, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", true}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KSchema:                              {name: "Schema", key: "schema", mro: []Kind{KSchema, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KLock:                                {name: "Lock", key: "lock", mro: []Kind{KLock, KExpression, KExpr}, args: []argSpec{{"update", true}, {"expressions", false}, {"wait", false}, {"key", false}}, flags: 0, sqlNames: []string{}},
	KSelect:                              {name: "Select", key: "select", mro: []Kind{KSelect, KExpression, KQuery, KSelectable, KExpr}, args: []argSpec{{"with_", false}, {"kind", false}, {"expressions", false}, {"hint", false}, {"distinct", false}, {"into", false}, {"from_", false}, {"operation_modifiers", false}, {"exclude", false}, {"match", false}, {"laterals", false}, {"joins", false}, {"connect", false}, {"pivots", false}, {"prewhere", false}, {"where", false}, {"group", false}, {"having", false}, {"qualify", false}, {"windows", false}, {"distribute", false}, {"sort", false}, {"cluster", false}, {"order", false}, {"limit", false}, {"offset", false}, {"locks", false}, {"sample", false}, {"settings", false}, {"format", false}, {"options", false}, {"for_", false}}, flags: 0, sqlNames: []string{}},
	KSubquery:                            {name: "Subquery", key: "subquery", mro: []Kind{KSubquery, KExpression, KDerivedTable, KQuery, KSelectable, KExpr}, args: []argSpec{{"this", true}, {"alias", false}, {"with_", false}, {"match", false}, {"laterals", false}, {"joins", false}, {"connect", false}, {"pivots", false}, {"prewhere", false}, {"where", false}, {"group", false}, {"having", false}, {"qualify", false}, {"windows", false}, {"distribute", false}, {"sort", false}, {"cluster", false}, {"order", false}, {"limit", false}, {"offset", false}, {"locks", false}, {"sample", false}, {"settings", false}, {"format", false}, {"options", false}, {"for_", false}}, flags: kfSubquery, sqlNames: []string{}},
	KTableSample:                         {name: "TableSample", key: "tablesample", mro: []Kind{KTableSample, KExpression, KExpr}, args: []argSpec{{"expressions", false}, {"method", false}, {"bucket_numerator", false}, {"bucket_denominator", false}, {"bucket_field", false}, {"percent", false}, {"rows", false}, {"size", false}, {"seed", false}}, flags: 0, sqlNames: []string{}},
	KTag:                                 {name: "Tag", key: "tag", mro: []Kind{KTag, KExpression, KExpr}, args: []argSpec{{"this", false}, {"prefix", false}, {"postfix", false}}, flags: 0, sqlNames: []string{}},
	KPivot:                               {name: "Pivot", key: "pivot", mro: []Kind{KPivot, KExpression, KExpr}, args: []argSpec{{"this", false}, {"alias", false}, {"expressions", false}, {"fields", false}, {"unpivot", false}, {"using", false}, {"group", false}, {"columns", false}, {"include_nulls", false}, {"default_on_null", false}, {"into", false}, {"with_", false}, {"identify_pivot_strings", false}, {"prefixed_pivot_columns", false}, {"pivot_column_naming", false}}, flags: 0, sqlNames: []string{}},
	KUnpivotColumns:                      {name: "UnpivotColumns", key: "unpivotcolumns", mro: []Kind{KUnpivotColumns, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: 0, sqlNames: []string{}},
	KWindow:                              {name: "Window", key: "window", mro: []Kind{KWindow, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}, {"partition_by", false}, {"order", false}, {"spec", false}, {"alias", false}, {"over", false}, {"first", false}}, flags: 0, sqlNames: []string{}},
	KWindowSpec:                          {name: "WindowSpec", key: "windowspec", mro: []Kind{KWindowSpec, KExpression, KExpr}, args: []argSpec{{"kind", false}, {"start", false}, {"start_side", false}, {"end", false}, {"end_side", false}, {"exclude", false}}, flags: 0, sqlNames: []string{}},
	KPreWhere:                            {name: "PreWhere", key: "prewhere", mro: []Kind{KPreWhere, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KWhere:                               {name: "Where", key: "where", mro: []Kind{KWhere, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KAnalyze:                             {name: "Analyze", key: "analyze", mro: []Kind{KAnalyze, KExpression, KExpr}, args: []argSpec{{"kind", false}, {"this", false}, {"tables", false}, {"options", false}, {"mode", false}, {"partition", false}, {"expression", false}, {"properties", false}}, flags: 0, sqlNames: []string{}},
	KAnalyzeStatistics:                   {name: "AnalyzeStatistics", key: "analyzestatistics", mro: []Kind{KAnalyzeStatistics, KExpression, KExpr}, args: []argSpec{{"kind", true}, {"option", false}, {"this", false}, {"expressions", false}}, flags: 0, sqlNames: []string{}},
	KAnalyzeHistogram:                    {name: "AnalyzeHistogram", key: "analyzehistogram", mro: []Kind{KAnalyzeHistogram, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}, {"expression", false}, {"update_options", false}}, flags: 0, sqlNames: []string{}},
	KAnalyzeSample:                       {name: "AnalyzeSample", key: "analyzesample", mro: []Kind{KAnalyzeSample, KExpression, KExpr}, args: []argSpec{{"kind", true}, {"sample", true}}, flags: 0, sqlNames: []string{}},
	KAnalyzeListChainedRows:              {name: "AnalyzeListChainedRows", key: "analyzelistchainedrows", mro: []Kind{KAnalyzeListChainedRows, KExpression, KExpr}, args: []argSpec{{"expression", false}}, flags: 0, sqlNames: []string{}},
	KAnalyzeDelete:                       {name: "AnalyzeDelete", key: "analyzedelete", mro: []Kind{KAnalyzeDelete, KExpression, KExpr}, args: []argSpec{{"kind", false}}, flags: 0, sqlNames: []string{}},
	KAnalyzeWith:                         {name: "AnalyzeWith", key: "analyzewith", mro: []Kind{KAnalyzeWith, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KAnalyzeValidate:                     {name: "AnalyzeValidate", key: "analyzevalidate", mro: []Kind{KAnalyzeValidate, KExpression, KExpr}, args: []argSpec{{"kind", true}, {"this", false}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KAnalyzeColumns:                      {name: "AnalyzeColumns", key: "analyzecolumns", mro: []Kind{KAnalyzeColumns, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KUsingData:                           {name: "UsingData", key: "usingdata", mro: []Kind{KUsingData, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KAddPartition:                        {name: "AddPartition", key: "addpartition", mro: []Kind{KAddPartition, KExpression, KExpr}, args: []argSpec{{"this", true}, {"exists", false}, {"location", false}}, flags: 0, sqlNames: []string{}},
	KAttachOption:                        {name: "AttachOption", key: "attachoption", mro: []Kind{KAttachOption, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KDropPartition:                       {name: "DropPartition", key: "droppartition", mro: []Kind{KDropPartition, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"exists", false}}, flags: 0, sqlNames: []string{}},
	KReplacePartition:                    {name: "ReplacePartition", key: "replacepartition", mro: []Kind{KReplacePartition, KExpression, KExpr}, args: []argSpec{{"expression", true}, {"source", true}}, flags: 0, sqlNames: []string{}},
	KTranslateCharacters:                 {name: "TranslateCharacters", key: "translatecharacters", mro: []Kind{KTranslateCharacters, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"with_error", false}}, flags: 0, sqlNames: []string{}},
	KOverflowTruncateBehavior:            {name: "OverflowTruncateBehavior", key: "overflowtruncatebehavior", mro: []Kind{KOverflowTruncateBehavior, KExpression, KExpr}, args: []argSpec{{"this", false}, {"with_count", true}}, flags: 0, sqlNames: []string{}},
	KJSON:                                {name: "JSON", key: "json", mro: []Kind{KJSON, KExpression, KExpr}, args: []argSpec{{"this", false}, {"with_", false}, {"unique", false}}, flags: 0, sqlNames: []string{}},
	KJSONPath:                            {name: "JSONPath", key: "jsonpath", mro: []Kind{KJSONPath, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KJSONPathPart:                        {name: "JSONPathPart", key: "jsonpathpart", mro: []Kind{KJSONPathPart, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KJSONPathFilter:                      {name: "JSONPathFilter", key: "jsonpathfilter", mro: []Kind{KJSONPathFilter, KJSONPathPart, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KJSONPathKey:                         {name: "JSONPathKey", key: "jsonpathkey", mro: []Kind{KJSONPathKey, KJSONPathPart, KExpression, KExpr}, args: []argSpec{{"this", true}, {"quoted", false}}, flags: 0, sqlNames: []string{}},
	KJSONPathRecursive:                   {name: "JSONPathRecursive", key: "jsonpathrecursive", mro: []Kind{KJSONPathRecursive, KJSONPathPart, KExpression, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{}},
	KJSONPathRoot:                        {name: "JSONPathRoot", key: "jsonpathroot", mro: []Kind{KJSONPathRoot, KJSONPathPart, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KJSONPathScript:                      {name: "JSONPathScript", key: "jsonpathscript", mro: []Kind{KJSONPathScript, KJSONPathPart, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KJSONPathSlice:                       {name: "JSONPathSlice", key: "jsonpathslice", mro: []Kind{KJSONPathSlice, KJSONPathPart, KExpression, KExpr}, args: []argSpec{{"start", false}, {"end", false}, {"step", false}}, flags: 0, sqlNames: []string{}},
	KJSONPathSelector:                    {name: "JSONPathSelector", key: "jsonpathselector", mro: []Kind{KJSONPathSelector, KJSONPathPart, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KJSONPathSubscript:                   {name: "JSONPathSubscript", key: "jsonpathsubscript", mro: []Kind{KJSONPathSubscript, KJSONPathPart, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KJSONPathUnion:                       {name: "JSONPathUnion", key: "jsonpathunion", mro: []Kind{KJSONPathUnion, KJSONPathPart, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KJSONPathWildcard:                    {name: "JSONPathWildcard", key: "jsonpathwildcard", mro: []Kind{KJSONPathWildcard, KJSONPathPart, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KFormatJson:                          {name: "FormatJson", key: "formatjson", mro: []Kind{KFormatJson, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KJSONKeyValue:                        {name: "JSONKeyValue", key: "jsonkeyvalue", mro: []Kind{KJSONKeyValue, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KJSONColumnDef:                       {name: "JSONColumnDef", key: "jsoncolumndef", mro: []Kind{KJSONColumnDef, KExpression, KExpr}, args: []argSpec{{"this", false}, {"kind", false}, {"path", false}, {"nested_schema", false}, {"ordinality", false}, {"format_json", false}}, flags: 0, sqlNames: []string{}},
	KJSONSchema:                          {name: "JSONSchema", key: "jsonschema", mro: []Kind{KJSONSchema, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KJSONValue:                           {name: "JSONValue", key: "jsonvalue", mro: []Kind{KJSONValue, KExpression, KExpr}, args: []argSpec{{"this", true}, {"path", true}, {"returning", false}, {"on_condition", false}}, flags: 0, sqlNames: []string{}},
	KJSONValueArray:                      {name: "JSONValueArray", key: "jsonvaluearray", mro: []Kind{KJSONValueArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"J_S_O_N_VALUE_ARRAY"}},
	KOpenJSONColumnDef:                   {name: "OpenJSONColumnDef", key: "openjsoncolumndef", mro: []Kind{KOpenJSONColumnDef, KExpression, KExpr}, args: []argSpec{{"this", true}, {"kind", true}, {"path", false}, {"as_json", false}}, flags: 0, sqlNames: []string{}},
	KJSONExtractQuote:                    {name: "JSONExtractQuote", key: "jsonextractquote", mro: []Kind{KJSONExtractQuote, KExpression, KExpr}, args: []argSpec{{"option", true}, {"scalar", false}}, flags: 0, sqlNames: []string{}},
	KScopeResolution:                     {name: "ScopeResolution", key: "scoperesolution", mro: []Kind{KScopeResolution, KExpression, KExpr}, args: []argSpec{{"this", false}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KStream:                              {name: "Stream", key: "stream", mro: []Kind{KStream, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KModelAttribute:                      {name: "ModelAttribute", key: "modelattribute", mro: []Kind{KModelAttribute, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KXMLNamespace:                        {name: "XMLNamespace", key: "xmlnamespace", mro: []Kind{KXMLNamespace, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KXMLKeyValueOption:                   {name: "XMLKeyValueOption", key: "xmlkeyvalueoption", mro: []Kind{KXMLKeyValueOption, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{}},
	KSemicolon:                           {name: "Semicolon", key: "semicolon", mro: []Kind{KSemicolon, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KTableColumn:                         {name: "TableColumn", key: "tablecolumn", mro: []Kind{KTableColumn, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KVariadic:                            {name: "Variadic", key: "variadic", mro: []Kind{KVariadic, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KStoredProcedure:                     {name: "StoredProcedure", key: "storedprocedure", mro: []Kind{KStoredProcedure, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"wrapped", false}}, flags: 0, sqlNames: []string{}},
	KBlock:                               {name: "Block", key: "block", mro: []Kind{KBlock, KExpression, KExpr}, args: []argSpec{{"expressions", true}, {"begin", false}}, flags: 0, sqlNames: []string{}},
	KIfBlock:                             {name: "IfBlock", key: "ifblock", mro: []Kind{KIfBlock, KExpression, KExpr}, args: []argSpec{{"this", true}, {"true", true}, {"false", false}}, flags: 0, sqlNames: []string{}},
	KWhileBlock:                          {name: "WhileBlock", key: "whileblock", mro: []Kind{KWhileBlock, KExpression, KExpr}, args: []argSpec{{"this", true}, {"body", true}, {"label", false}}, flags: 0, sqlNames: []string{}},
	KEndStatement:                        {name: "EndStatement", key: "endstatement", mro: []Kind{KEndStatement, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KAscii:                               {name: "Ascii", key: "ascii", mro: []Kind{KAscii, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"ASCII"}},
	KBitLength:                           {name: "BitLength", key: "bitlength", mro: []Kind{KBitLength, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BIT_LENGTH"}},
	KByteLength:                          {name: "ByteLength", key: "bytelength", mro: []Kind{KByteLength, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"BYTE_LENGTH"}},
	KChr:                                 {name: "Chr", key: "chr", mro: []Kind{KChr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}, {"charset", false}}, flags: kfVarLenArgs, sqlNames: []string{"CHR", "CHAR"}},
	KConcat:                              {name: "Concat", key: "concat", mro: []Kind{KConcat, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}, {"safe", false}, {"coalesce", false}}, flags: kfVarLenArgs, sqlNames: []string{"CONCAT"}},
	KConcatWs:                            {name: "ConcatWs", key: "concatws", mro: []Kind{KConcatWs, KConcat, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}, {"safe", false}, {"coalesce", false}}, flags: kfVarLenArgs, sqlNames: []string{"CONCAT_WS"}},
	KContains:                            {name: "Contains", key: "contains", mro: []Kind{KContains, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"json_scope", false}}, flags: 0, sqlNames: []string{"CONTAINS"}},
	KElt:                                 {name: "Elt", key: "elt", mro: []Kind{KElt, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"ELT"}},
	KEndsWith:                            {name: "EndsWith", key: "endswith", mro: []Kind{KEndsWith, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"ENDS_WITH", "ENDSWITH"}},
	KFormat:                              {name: "Format", key: "format", mro: []Kind{KFormat, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}, {"is_fmt", false}}, flags: kfVarLenArgs, sqlNames: []string{"FORMAT"}},
	KInitcap:                             {name: "Initcap", key: "initcap", mro: []Kind{KInitcap, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"INITCAP"}},
	KIsAscii:                             {name: "IsAscii", key: "isascii", mro: []Kind{KIsAscii, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"IS_ASCII"}},
	KLeft:                                {name: "Left", key: "left", mro: []Kind{KLeft, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"negative_length_returns_empty", false}}, flags: 0, sqlNames: []string{"LEFT"}},
	KLength:                              {name: "Length", key: "length", mro: []Kind{KLength, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"binary", false}, {"encoding", false}}, flags: 0, sqlNames: []string{"LENGTH", "LEN", "CHAR_LENGTH", "CHARACTER_LENGTH"}},
	KLevenshtein:                         {name: "Levenshtein", key: "levenshtein", mro: []Kind{KLevenshtein, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"ins_cost", false}, {"del_cost", false}, {"sub_cost", false}, {"max_dist", false}}, flags: 0, sqlNames: []string{"LEVENSHTEIN"}},
	KLower:                               {name: "Lower", key: "lower", mro: []Kind{KLower, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"LOWER", "LCASE"}},
	KMatchAgainst:                        {name: "MatchAgainst", key: "matchagainst", mro: []Kind{KMatchAgainst, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}, {"modifier", false}}, flags: 0, sqlNames: []string{"MATCH_AGAINST"}},
	KNormalize:                           {name: "Normalize", key: "normalize", mro: []Kind{KNormalize, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"form", false}, {"is_casefold", false}}, flags: 0, sqlNames: []string{"NORMALIZE"}},
	KNumberToStr:                         {name: "NumberToStr", key: "numbertostr", mro: []Kind{KNumberToStr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", true}, {"culture", false}}, flags: 0, sqlNames: []string{"NUMBER_TO_STR"}},
	KOverlay:                             {name: "Overlay", key: "overlay", mro: []Kind{KOverlay, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"from_", true}, {"for_", false}}, flags: 0, sqlNames: []string{"OVERLAY"}},
	KPad:                                 {name: "Pad", key: "pad", mro: []Kind{KPad, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"fill_pattern", false}, {"is_left", true}}, flags: 0, sqlNames: []string{"PAD"}},
	KRepeat:                              {name: "Repeat", key: "repeat", mro: []Kind{KRepeat, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"times", true}}, flags: 0, sqlNames: []string{"REPEAT"}},
	KReplace:                             {name: "Replace", key: "replace", mro: []Kind{KReplace, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"replacement", false}}, flags: 0, sqlNames: []string{"REPLACE"}},
	KReverse:                             {name: "Reverse", key: "reverse", mro: []Kind{KReverse, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"REVERSE"}},
	KRight:                               {name: "Right", key: "right", mro: []Kind{KRight, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"negative_length_returns_empty", false}}, flags: 0, sqlNames: []string{"RIGHT"}},
	KRtrimmedLength:                      {name: "RtrimmedLength", key: "rtrimmedlength", mro: []Kind{KRtrimmedLength, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"RTRIMMED_LENGTH"}},
	KSearch:                              {name: "Search", key: "search", mro: []Kind{KSearch, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"json_scope", false}, {"analyzer", false}, {"analyzer_options", false}, {"search_mode", false}}, flags: 0, sqlNames: []string{"SEARCH"}},
	KSearchIp:                            {name: "SearchIp", key: "searchip", mro: []Kind{KSearchIp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"SEARCH_IP"}},
	KSoundex:                             {name: "Soundex", key: "soundex", mro: []Kind{KSoundex, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SOUNDEX"}},
	KSoundexP123:                         {name: "SoundexP123", key: "soundexp123", mro: []Kind{KSoundexP123, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SOUNDEX_P123"}},
	KSpace:                               {name: "Space", key: "space", mro: []Kind{KSpace, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SPACE"}},
	KSplit:                               {name: "Split", key: "split", mro: []Kind{KSplit, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"limit", false}, {"null_returns_null", false}, {"empty_delimiter_returns_whole", false}}, flags: 0, sqlNames: []string{"SPLIT"}},
	KSplitPart:                           {name: "SplitPart", key: "splitpart", mro: []Kind{KSplitPart, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"delimiter", false}, {"part_index", false}, {"part_index_zero_as_one", false}, {"empty_delimiter_returns_whole", false}}, flags: 0, sqlNames: []string{"SPLIT_PART"}},
	KStrtok:                              {name: "Strtok", key: "strtok", mro: []Kind{KStrtok, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"delimiter", false}, {"part_index", false}}, flags: 0, sqlNames: []string{"STRTOK"}},
	KStartsWith:                          {name: "StartsWith", key: "startswith", mro: []Kind{KStartsWith, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"STARTS_WITH", "STARTSWITH"}},
	KStrPosition:                         {name: "StrPosition", key: "strposition", mro: []Kind{KStrPosition, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"substr", true}, {"position", false}, {"occurrence", false}, {"clamp_position", false}}, flags: 0, sqlNames: []string{"STR_POSITION"}},
	KStrToMap:                            {name: "StrToMap", key: "strtomap", mro: []Kind{KStrToMap, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"pair_delim", false}, {"key_value_delim", false}, {"duplicate_resolution_callback", false}}, flags: 0, sqlNames: []string{"STR_TO_MAP"}},
	KString:                              {name: "String", key: "string", mro: []Kind{KString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"zone", false}}, flags: 0, sqlNames: []string{"STRING"}},
	KStuff:                               {name: "Stuff", key: "stuff", mro: []Kind{KStuff, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"start", true}, {"length", true}, {"expression", true}}, flags: 0, sqlNames: []string{"STUFF", "INSERT"}},
	KSubstring:                           {name: "Substring", key: "substring", mro: []Kind{KSubstring, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"start", false}, {"length", false}, {"zero_start", false}}, flags: 0, sqlNames: []string{"SUBSTRING", "SUBSTR"}},
	KSubstringIndex:                      {name: "SubstringIndex", key: "substringindex", mro: []Kind{KSubstringIndex, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"delimiter", true}, {"count", true}}, flags: 0, sqlNames: []string{"SUBSTRING_INDEX"}},
	KTranslate:                           {name: "Translate", key: "translate", mro: []Kind{KTranslate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"from_", true}, {"to", true}}, flags: 0, sqlNames: []string{"TRANSLATE"}},
	KTrim:                                {name: "Trim", key: "trim", mro: []Kind{KTrim, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}, {"position", false}, {"collation", false}}, flags: 0, sqlNames: []string{"TRIM"}},
	KUnicode:                             {name: "Unicode", key: "unicode", mro: []Kind{KUnicode, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"empty_is_zero", false}}, flags: 0, sqlNames: []string{"UNICODE"}},
	KUpper:                               {name: "Upper", key: "upper", mro: []Kind{KUpper, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"UPPER", "UCASE"}},
	KBase64DecodeBinary:                  {name: "Base64DecodeBinary", key: "base64decodebinary", mro: []Kind{KBase64DecodeBinary, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"alphabet", false}}, flags: 0, sqlNames: []string{"BASE64_DECODE_BINARY"}},
	KBase64DecodeString:                  {name: "Base64DecodeString", key: "base64decodestring", mro: []Kind{KBase64DecodeString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"alphabet", false}}, flags: 0, sqlNames: []string{"BASE64_DECODE_STRING"}},
	KBase64Encode:                        {name: "Base64Encode", key: "base64encode", mro: []Kind{KBase64Encode, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"max_line_length", false}, {"alphabet", false}}, flags: 0, sqlNames: []string{"BASE64_ENCODE"}},
	KCodePointsToBytes:                   {name: "CodePointsToBytes", key: "codepointstobytes", mro: []Kind{KCodePointsToBytes, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"CODE_POINTS_TO_BYTES"}},
	KCodePointsToString:                  {name: "CodePointsToString", key: "codepointstostring", mro: []Kind{KCodePointsToString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"CODE_POINTS_TO_STRING"}},
	KConvertToCharset:                    {name: "ConvertToCharset", key: "converttocharset", mro: []Kind{KConvertToCharset, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"dest", true}, {"source", false}}, flags: 0, sqlNames: []string{"CONVERT_TO_CHARSET"}},
	KDecode:                              {name: "Decode", key: "decode", mro: []Kind{KDecode, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"charset", true}, {"replace", false}}, flags: 0, sqlNames: []string{"DECODE"}},
	KEncode:                              {name: "Encode", key: "encode", mro: []Kind{KEncode, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"charset", true}}, flags: 0, sqlNames: []string{"ENCODE"}},
	KFromBase:                            {name: "FromBase", key: "frombase", mro: []Kind{KFromBase, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"FROM_BASE"}},
	KFromBase32:                          {name: "FromBase32", key: "frombase32", mro: []Kind{KFromBase32, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"FROM_BASE32"}},
	KFromBase64:                          {name: "FromBase64", key: "frombase64", mro: []Kind{KFromBase64, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"FROM_BASE64"}},
	KHex:                                 {name: "Hex", key: "hex", mro: []Kind{KHex, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"case", false}}, flags: 0, sqlNames: []string{"HEX"}},
	KHexDecodeString:                     {name: "HexDecodeString", key: "hexdecodestring", mro: []Kind{KHexDecodeString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"HEX_DECODE_STRING"}},
	KLowerHex:                            {name: "LowerHex", key: "lowerhex", mro: []Kind{KLowerHex, KHex, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"case", false}}, flags: 0, sqlNames: []string{"LOWER_HEX"}},
	KSafeConvertBytesToString:            {name: "SafeConvertBytesToString", key: "safeconvertbytestostring", mro: []Kind{KSafeConvertBytesToString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SAFE_CONVERT_BYTES_TO_STRING"}},
	KToBase32:                            {name: "ToBase32", key: "tobase32", mro: []Kind{KToBase32, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TO_BASE32"}},
	KToBase64:                            {name: "ToBase64", key: "tobase64", mro: []Kind{KToBase64, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TO_BASE64"}},
	KToBinary:                            {name: "ToBinary", key: "tobinary", mro: []Kind{KToBinary, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}, {"safe", false}}, flags: 0, sqlNames: []string{"TO_BINARY"}},
	KToChar:                              {name: "ToChar", key: "tochar", mro: []Kind{KToChar, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}, {"nlsparam", false}, {"is_numeric", false}}, flags: 0, sqlNames: []string{"TO_CHAR"}},
	KToCodePoints:                        {name: "ToCodePoints", key: "tocodepoints", mro: []Kind{KToCodePoints, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TO_CODE_POINTS"}},
	KToDecfloat:                          {name: "ToDecfloat", key: "todecfloat", mro: []Kind{KToDecfloat, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}}, flags: 0, sqlNames: []string{"TO_DECFLOAT"}},
	KToDouble:                            {name: "ToDouble", key: "todouble", mro: []Kind{KToDouble, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}, {"safe", false}}, flags: 0, sqlNames: []string{"TO_DOUBLE"}},
	KToFile:                              {name: "ToFile", key: "tofile", mro: []Kind{KToFile, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"path", false}, {"safe", false}}, flags: 0, sqlNames: []string{"TO_FILE"}},
	KToNumber:                            {name: "ToNumber", key: "tonumber", mro: []Kind{KToNumber, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}, {"nlsparam", false}, {"precision", false}, {"scale", false}, {"safe", false}, {"safe_name", false}, {"default", false}}, flags: 0, sqlNames: []string{"TO_NUMBER"}},
	KTryBase64DecodeBinary:               {name: "TryBase64DecodeBinary", key: "trybase64decodebinary", mro: []Kind{KTryBase64DecodeBinary, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"alphabet", false}}, flags: 0, sqlNames: []string{"TRY_BASE64_DECODE_BINARY"}},
	KTryBase64DecodeString:               {name: "TryBase64DecodeString", key: "trybase64decodestring", mro: []Kind{KTryBase64DecodeString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"alphabet", false}}, flags: 0, sqlNames: []string{"TRY_BASE64_DECODE_STRING"}},
	KTryHexDecodeBinary:                  {name: "TryHexDecodeBinary", key: "tryhexdecodebinary", mro: []Kind{KTryHexDecodeBinary, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TRY_HEX_DECODE_BINARY"}},
	KTryHexDecodeString:                  {name: "TryHexDecodeString", key: "tryhexdecodestring", mro: []Kind{KTryHexDecodeString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TRY_HEX_DECODE_STRING"}},
	KTryToDecfloat:                       {name: "TryToDecfloat", key: "trytodecfloat", mro: []Kind{KTryToDecfloat, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}}, flags: 0, sqlNames: []string{"TRY_TO_DECFLOAT"}},
	KUnhex:                               {name: "Unhex", key: "unhex", mro: []Kind{KUnhex, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"UNHEX"}},
	KRegexpCount:                         {name: "RegexpCount", key: "regexpcount", mro: []Kind{KRegexpCount, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"position", false}, {"parameters", false}}, flags: 0, sqlNames: []string{"REGEXP_COUNT"}},
	KRegexpExtract:                       {name: "RegexpExtract", key: "regexpextract", mro: []Kind{KRegexpExtract, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"position", false}, {"occurrence", false}, {"parameters", false}, {"group", false}, {"null_if_pos_overflow", false}, {"inferred_group", false}}, flags: 0, sqlNames: []string{"REGEXP_EXTRACT"}},
	KRegexpExtractAll:                    {name: "RegexpExtractAll", key: "regexpextractall", mro: []Kind{KRegexpExtractAll, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"group", false}, {"parameters", false}, {"position", false}, {"occurrence", false}, {"inferred_group", false}}, flags: 0, sqlNames: []string{"REGEXP_EXTRACT_ALL"}},
	KRegexpFullMatch:                     {name: "RegexpFullMatch", key: "regexpfullmatch", mro: []Kind{KRegexpFullMatch, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"options", false}}, flags: 0, sqlNames: []string{"REGEXP_FULL_MATCH"}},
	KRegexpILike:                         {name: "RegexpILike", key: "regexpilike", mro: []Kind{KRegexpILike, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"flag", false}}, flags: 0, sqlNames: []string{"REGEXP_I_LIKE"}},
	KRegexpInstr:                         {name: "RegexpInstr", key: "regexpinstr", mro: []Kind{KRegexpInstr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"position", false}, {"occurrence", false}, {"option", false}, {"parameters", false}, {"group", false}}, flags: 0, sqlNames: []string{"REGEXP_INSTR"}},
	KRegexpReplace:                       {name: "RegexpReplace", key: "regexpreplace", mro: []Kind{KRegexpReplace, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"replacement", false}, {"position", false}, {"occurrence", false}, {"modifiers", false}, {"single_replace", false}}, flags: 0, sqlNames: []string{"REGEXP_REPLACE"}},
	KRegexpSplit:                         {name: "RegexpSplit", key: "regexpsplit", mro: []Kind{KRegexpSplit, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"limit", false}}, flags: 0, sqlNames: []string{"REGEXP_SPLIT"}},
	KRegexpSubstr:                        {name: "RegexpSubstr", key: "regexpsubstr", mro: []Kind{KRegexpSubstr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"position", false}, {"occurrence", false}, {"parameters", false}, {"group", false}}, flags: 0, sqlNames: []string{"REGEXP_SUBSTR"}},
	KCompress:                            {name: "Compress", key: "compress", mro: []Kind{KCompress, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"method", false}}, flags: 0, sqlNames: []string{"COMPRESS"}},
	KDecrypt:                             {name: "Decrypt", key: "decrypt", mro: []Kind{KDecrypt, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"passphrase", true}, {"aad", false}, {"encryption_method", false}, {"safe", false}}, flags: 0, sqlNames: []string{"DECRYPT"}},
	KDecryptRaw:                          {name: "DecryptRaw", key: "decryptraw", mro: []Kind{KDecryptRaw, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"key", true}, {"iv", true}, {"aad", false}, {"encryption_method", false}, {"aead", false}, {"safe", false}}, flags: 0, sqlNames: []string{"DECRYPT_RAW"}},
	KDecompressBinary:                    {name: "DecompressBinary", key: "decompressbinary", mro: []Kind{KDecompressBinary, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"method", true}}, flags: 0, sqlNames: []string{"DECOMPRESS_BINARY"}},
	KDecompressString:                    {name: "DecompressString", key: "decompressstring", mro: []Kind{KDecompressString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"method", true}}, flags: 0, sqlNames: []string{"DECOMPRESS_STRING"}},
	KEncrypt:                             {name: "Encrypt", key: "encrypt", mro: []Kind{KEncrypt, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"passphrase", true}, {"aad", false}, {"encryption_method", false}}, flags: 0, sqlNames: []string{"ENCRYPT"}},
	KEncryptRaw:                          {name: "EncryptRaw", key: "encryptraw", mro: []Kind{KEncryptRaw, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"key", true}, {"iv", true}, {"aad", false}, {"encryption_method", false}}, flags: 0, sqlNames: []string{"ENCRYPT_RAW"}},
	KCityHash64:                          {name: "CityHash64", key: "cityhash64", mro: []Kind{KCityHash64, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"CITY_HASH64"}},
	KFarmFingerprint:                     {name: "FarmFingerprint", key: "farmfingerprint", mro: []Kind{KFarmFingerprint, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"expressions", true}}, flags: kfVarLenArgs, sqlNames: []string{"FARM_FINGERPRINT", "FARMFINGERPRINT64"}},
	KMD5:                                 {name: "MD5", key: "md5", mro: []Kind{KMD5, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"MD5"}},
	KMD5Digest:                           {name: "MD5Digest", key: "md5digest", mro: []Kind{KMD5Digest, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"MD5_DIGEST"}},
	KMD5NumberLower64:                    {name: "MD5NumberLower64", key: "md5numberlower64", mro: []Kind{KMD5NumberLower64, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"M_D5_NUMBER_LOWER64"}},
	KMD5NumberUpper64:                    {name: "MD5NumberUpper64", key: "md5numberupper64", mro: []Kind{KMD5NumberUpper64, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"M_D5_NUMBER_UPPER64"}},
	KSHA:                                 {name: "SHA", key: "sha", mro: []Kind{KSHA, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SHA", "SHA1"}},
	KSHA1Digest:                          {name: "SHA1Digest", key: "sha1digest", mro: []Kind{KSHA1Digest, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"S_H_A1_DIGEST"}},
	KSHA2:                                {name: "SHA2", key: "sha2", mro: []Kind{KSHA2, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"length", false}}, flags: 0, sqlNames: []string{"SHA2"}},
	KSHA2Digest:                          {name: "SHA2Digest", key: "sha2digest", mro: []Kind{KSHA2Digest, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"length", false}}, flags: 0, sqlNames: []string{"S_H_A2_DIGEST"}},
	KStandardHash:                        {name: "StandardHash", key: "standardhash", mro: []Kind{KStandardHash, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"STANDARD_HASH"}},
	KParseBignumeric:                     {name: "ParseBignumeric", key: "parsebignumeric", mro: []Kind{KParseBignumeric, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"PARSE_BIGNUMERIC"}},
	KParseNumeric:                        {name: "ParseNumeric", key: "parsenumeric", mro: []Kind{KParseNumeric, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"PARSE_NUMERIC"}},
	KParseUrl:                            {name: "ParseUrl", key: "parseurl", mro: []Kind{KParseUrl, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"part_to_extract", false}, {"key", false}, {"permissive", false}}, flags: 0, sqlNames: []string{"PARSE_URL"}},
	KCurrentDate:                         {name: "CurrentDate", key: "currentdate", mro: []Kind{KCurrentDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"CURRENT_DATE"}},
	KCurrentDatetime:                     {name: "CurrentDatetime", key: "currentdatetime", mro: []Kind{KCurrentDatetime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"CURRENT_DATETIME"}},
	KCurrentTime:                         {name: "CurrentTime", key: "currenttime", mro: []Kind{KCurrentTime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"CURRENT_TIME"}},
	KCurrentTimestamp:                    {name: "CurrentTimestamp", key: "currenttimestamp", mro: []Kind{KCurrentTimestamp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"sysdate", false}}, flags: 0, sqlNames: []string{"CURRENT_TIMESTAMP"}},
	KCurrentTimestampLTZ:                 {name: "CurrentTimestampLTZ", key: "currenttimestampltz", mro: []Kind{KCurrentTimestampLTZ, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_TIMESTAMP_L_T_Z"}},
	KCurrentTimezone:                     {name: "CurrentTimezone", key: "currenttimezone", mro: []Kind{KCurrentTimezone, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"CURRENT_TIMEZONE"}},
	KLocaltime:                           {name: "Localtime", key: "localtime", mro: []Kind{KLocaltime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"LOCALTIME"}},
	KLocaltimestamp:                      {name: "Localtimestamp", key: "localtimestamp", mro: []Kind{KLocaltimestamp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"LOCALTIMESTAMP"}},
	KSystimestamp:                        {name: "Systimestamp", key: "systimestamp", mro: []Kind{KSystimestamp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"SYSTIMESTAMP"}},
	KUtcDate:                             {name: "UtcDate", key: "utcdate", mro: []Kind{KUtcDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{"UTC_DATE"}},
	KUtcTime:                             {name: "UtcTime", key: "utctime", mro: []Kind{KUtcTime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"UTC_TIME"}},
	KUtcTimestamp:                        {name: "UtcTimestamp", key: "utctimestamp", mro: []Kind{KUtcTimestamp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}}, flags: 0, sqlNames: []string{"UTC_TIMESTAMP"}},
	KAddMonths:                           {name: "AddMonths", key: "addmonths", mro: []Kind{KAddMonths, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"preserve_end_of_month", false}}, flags: 0, sqlNames: []string{"ADD_MONTHS"}},
	KDateAdd:                             {name: "DateAdd", key: "dateadd", mro: []Kind{KDateAdd, KExpression, KFunc, KCondition, KIntervalOp, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"DATE_ADD"}},
	KDateBin:                             {name: "DateBin", key: "datebin", mro: []Kind{KDateBin, KExpression, KFunc, KCondition, KIntervalOp, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}, {"zone", false}, {"origin", false}}, flags: 0, sqlNames: []string{"DATE_BIN"}},
	KDateDiff:                            {name: "DateDiff", key: "datediff", mro: []Kind{KDateDiff, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}, {"zone", false}, {"big_int", false}, {"date_part_boundary", false}}, flags: 0, sqlNames: []string{"DATEDIFF", "DATE_DIFF"}},
	KDateSub:                             {name: "DateSub", key: "datesub", mro: []Kind{KDateSub, KExpression, KFunc, KCondition, KIntervalOp, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"DATE_SUB"}},
	KDatetimeAdd:                         {name: "DatetimeAdd", key: "datetimeadd", mro: []Kind{KDatetimeAdd, KExpression, KFunc, KCondition, KIntervalOp, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"DATETIME_ADD"}},
	KDatetimeDiff:                        {name: "DatetimeDiff", key: "datetimediff", mro: []Kind{KDatetimeDiff, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}, {"date_part_boundary", false}}, flags: 0, sqlNames: []string{"DATETIME_DIFF"}},
	KDatetimeSub:                         {name: "DatetimeSub", key: "datetimesub", mro: []Kind{KDatetimeSub, KExpression, KFunc, KCondition, KIntervalOp, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"DATETIME_SUB"}},
	KMonthsBetween:                       {name: "MonthsBetween", key: "monthsbetween", mro: []Kind{KMonthsBetween, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"roundoff", false}}, flags: 0, sqlNames: []string{"MONTHS_BETWEEN"}},
	KTimeAdd:                             {name: "TimeAdd", key: "timeadd", mro: []Kind{KTimeAdd, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"TIME_ADD"}},
	KTimeDiff:                            {name: "TimeDiff", key: "timediff", mro: []Kind{KTimeDiff, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"TIME_DIFF"}},
	KTimeSub:                             {name: "TimeSub", key: "timesub", mro: []Kind{KTimeSub, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"TIME_SUB"}},
	KTimestampAdd:                        {name: "TimestampAdd", key: "timestampadd", mro: []Kind{KTimestampAdd, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"TIMESTAMP_ADD"}},
	KTimestampDiff:                       {name: "TimestampDiff", key: "timestampdiff", mro: []Kind{KTimestampDiff, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"TIMESTAMPDIFF", "TIMESTAMP_DIFF"}},
	KTimestampSub:                        {name: "TimestampSub", key: "timestampsub", mro: []Kind{KTimestampSub, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"TIMESTAMP_SUB"}},
	KTsOrDsAdd:                           {name: "TsOrDsAdd", key: "tsordsadd", mro: []Kind{KTsOrDsAdd, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}, {"return_type", false}}, flags: 0, sqlNames: []string{"TS_OR_DS_ADD"}},
	KTsOrDsDiff:                          {name: "TsOrDsDiff", key: "tsordsdiff", mro: []Kind{KTsOrDsDiff, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", false}}, flags: 0, sqlNames: []string{"TS_OR_DS_DIFF"}},
	KDatetimeTrunc:                       {name: "DatetimeTrunc", key: "datetimetrunc", mro: []Kind{KDatetimeTrunc, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"unit", true}, {"zone", false}}, flags: 0, sqlNames: []string{"DATETIME_TRUNC"}},
	KDateTrunc:                           {name: "DateTrunc", key: "datetrunc", mro: []Kind{KDateTrunc, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"unit", true}, {"this", true}, {"zone", false}, {"input_type_preserved", false}}, flags: 0, sqlNames: []string{"DATE_TRUNC"}},
	KTimestampTrunc:                      {name: "TimestampTrunc", key: "timestamptrunc", mro: []Kind{KTimestampTrunc, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"unit", true}, {"zone", false}, {"input_type_preserved", false}}, flags: 0, sqlNames: []string{"TIMESTAMP_TRUNC"}},
	KTimeSlice:                           {name: "TimeSlice", key: "timeslice", mro: []Kind{KTimeSlice, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"expression", true}, {"unit", true}, {"kind", false}, {"origin", false}}, flags: 0, sqlNames: []string{"TIME_SLICE"}},
	KTimeTrunc:                           {name: "TimeTrunc", key: "timetrunc", mro: []Kind{KTimeTrunc, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"unit", true}, {"zone", false}}, flags: 0, sqlNames: []string{"TIME_TRUNC"}},
	KDay:                                 {name: "Day", key: "day", mro: []Kind{KDay, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DAY"}},
	KDayOfMonth:                          {name: "DayOfMonth", key: "dayofmonth", mro: []Kind{KDayOfMonth, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DAY_OF_MONTH", "DAYOFMONTH"}},
	KDayOfWeek:                           {name: "DayOfWeek", key: "dayofweek", mro: []Kind{KDayOfWeek, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DAY_OF_WEEK", "DAYOFWEEK"}},
	KDayOfWeekIso:                        {name: "DayOfWeekIso", key: "dayofweekiso", mro: []Kind{KDayOfWeekIso, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DAYOFWEEK_ISO", "ISODOW"}},
	KDayOfYear:                           {name: "DayOfYear", key: "dayofyear", mro: []Kind{KDayOfYear, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DAY_OF_YEAR", "DAYOFYEAR"}},
	KDayname:                             {name: "Dayname", key: "dayname", mro: []Kind{KDayname, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"abbreviated", false}}, flags: 0, sqlNames: []string{"DAYNAME"}},
	KExtract:                             {name: "Extract", key: "extract", mro: []Kind{KExtract, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"EXTRACT"}},
	KGetExtract:                          {name: "GetExtract", key: "getextract", mro: []Kind{KGetExtract, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"GET_EXTRACT"}},
	KHour:                                {name: "Hour", key: "hour", mro: []Kind{KHour, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"HOUR"}},
	KMinute:                              {name: "Minute", key: "minute", mro: []Kind{KMinute, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"MINUTE"}},
	KMonth:                               {name: "Month", key: "month", mro: []Kind{KMonth, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"MONTH"}},
	KMonthname:                           {name: "Monthname", key: "monthname", mro: []Kind{KMonthname, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"abbreviated", false}}, flags: 0, sqlNames: []string{"MONTHNAME"}},
	KQuarter:                             {name: "Quarter", key: "quarter", mro: []Kind{KQuarter, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"QUARTER"}},
	KSecond:                              {name: "Second", key: "second", mro: []Kind{KSecond, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"SECOND"}},
	KToDays:                              {name: "ToDays", key: "todays", mro: []Kind{KToDays, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TO_DAYS"}},
	KWeek:                                {name: "Week", key: "week", mro: []Kind{KWeek, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"mode", false}}, flags: 0, sqlNames: []string{"WEEK"}},
	KWeekOfYear:                          {name: "WeekOfYear", key: "weekofyear", mro: []Kind{KWeekOfYear, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"WEEK_OF_YEAR", "WEEKOFYEAR"}},
	KYear:                                {name: "Year", key: "year", mro: []Kind{KYear, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"YEAR"}},
	KYearOfWeek:                          {name: "YearOfWeek", key: "yearofweek", mro: []Kind{KYearOfWeek, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"YEAR_OF_WEEK", "YEAROFWEEK"}},
	KYearOfWeekIso:                       {name: "YearOfWeekIso", key: "yearofweekiso", mro: []Kind{KYearOfWeekIso, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"YEAR_OF_WEEK_ISO", "YEAROFWEEKISO"}},
	KDate:                                {name: "Date", key: "date", mro: []Kind{KDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"zone", false}, {"expressions", false}}, flags: kfVarLenArgs, sqlNames: []string{"DATE"}},
	KDateFromParts:                       {name: "DateFromParts", key: "datefromparts", mro: []Kind{KDateFromParts, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"year", true}, {"month", false}, {"day", false}, {"allow_overflow", false}}, flags: 0, sqlNames: []string{"DATE_FROM_PARTS", "DATEFROMPARTS"}},
	KDateFromUnixDate:                    {name: "DateFromUnixDate", key: "datefromunixdate", mro: []Kind{KDateFromUnixDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DATE_FROM_UNIX_DATE"}},
	KDatetime:                            {name: "Datetime", key: "datetime", mro: []Kind{KDatetime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", false}}, flags: 0, sqlNames: []string{"DATETIME"}},
	KGapFill:                             {name: "GapFill", key: "gapfill", mro: []Kind{KGapFill, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"ts_column", true}, {"bucket_width", true}, {"partitioning_columns", false}, {"value_columns", false}, {"origin", false}, {"ignore_nulls", false}}, flags: 0, sqlNames: []string{"GAP_FILL"}},
	KGenerateDateArray:                   {name: "GenerateDateArray", key: "generatedatearray", mro: []Kind{KGenerateDateArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"start", true}, {"end", true}, {"step", false}}, flags: 0, sqlNames: []string{"GENERATE_DATE_ARRAY"}},
	KGenerateTimestampArray:              {name: "GenerateTimestampArray", key: "generatetimestamparray", mro: []Kind{KGenerateTimestampArray, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"start", true}, {"end", true}, {"step", true}}, flags: 0, sqlNames: []string{"GENERATE_TIMESTAMP_ARRAY"}},
	KJustifyDays:                         {name: "JustifyDays", key: "justifydays", mro: []Kind{KJustifyDays, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"JUSTIFY_DAYS"}},
	KJustifyHours:                        {name: "JustifyHours", key: "justifyhours", mro: []Kind{KJustifyHours, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"JUSTIFY_HOURS"}},
	KJustifyInterval:                     {name: "JustifyInterval", key: "justifyinterval", mro: []Kind{KJustifyInterval, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"JUSTIFY_INTERVAL"}},
	KLastDay:                             {name: "LastDay", key: "lastday", mro: []Kind{KLastDay, KExpression, KFunc, KCondition, KTimeUnit, KExpr}, args: []argSpec{{"this", true}, {"unit", false}}, flags: 0, sqlNames: []string{"LAST_DAY", "LAST_DAY_OF_MONTH"}},
	KMakeInterval:                        {name: "MakeInterval", key: "makeinterval", mro: []Kind{KMakeInterval, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"year", false}, {"month", false}, {"week", false}, {"day", false}, {"hour", false}, {"minute", false}, {"second", false}}, flags: 0, sqlNames: []string{"MAKE_INTERVAL"}},
	KNextDay:                             {name: "NextDay", key: "nextday", mro: []Kind{KNextDay, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"NEXT_DAY"}},
	KPreviousDay:                         {name: "PreviousDay", key: "previousday", mro: []Kind{KPreviousDay, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"PREVIOUS_DAY"}},
	KTime:                                {name: "Time", key: "time", mro: []Kind{KTime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"zone", false}}, flags: 0, sqlNames: []string{"TIME"}},
	KTimeFromParts:                       {name: "TimeFromParts", key: "timefromparts", mro: []Kind{KTimeFromParts, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"hour", true}, {"min", true}, {"sec", true}, {"nano", false}, {"fractions", false}, {"precision", false}, {"overflow", false}}, flags: 0, sqlNames: []string{"TIME_FROM_PARTS", "TIMEFROMPARTS"}},
	KTimestamp:                           {name: "Timestamp", key: "timestamp", mro: []Kind{KTimestamp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"zone", false}, {"with_tz", false}}, flags: 0, sqlNames: []string{"TIMESTAMP"}},
	KTimestampFromParts:                  {name: "TimestampFromParts", key: "timestampfromparts", mro: []Kind{KTimestampFromParts, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"year", false}, {"month", false}, {"day", false}, {"hour", false}, {"min", false}, {"sec", false}, {"nano", false}, {"zone", false}, {"milli", false}, {"this", false}, {"expression", false}}, flags: 0, sqlNames: []string{"TIMESTAMP_FROM_PARTS", "TIMESTAMPFROMPARTS"}},
	KTimestampLtzFromParts:               {name: "TimestampLtzFromParts", key: "timestampltzfromparts", mro: []Kind{KTimestampLtzFromParts, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"year", false}, {"month", false}, {"day", false}, {"hour", false}, {"min", false}, {"sec", false}, {"nano", false}}, flags: 0, sqlNames: []string{"TIMESTAMP_LTZ_FROM_PARTS", "TIMESTAMPLTZFROMPARTS"}},
	KTimestampTzFromParts:                {name: "TimestampTzFromParts", key: "timestamptzfromparts", mro: []Kind{KTimestampTzFromParts, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"year", false}, {"month", false}, {"day", false}, {"hour", false}, {"min", false}, {"sec", false}, {"nano", false}, {"zone", false}}, flags: 0, sqlNames: []string{"TIMESTAMP_TZ_FROM_PARTS", "TIMESTAMPTZFROMPARTS"}},
	KConvertTimezone:                     {name: "ConvertTimezone", key: "converttimezone", mro: []Kind{KConvertTimezone, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"source_tz", false}, {"target_tz", true}, {"timestamp", true}, {"options", false}}, flags: 0, sqlNames: []string{"CONVERT_TIMEZONE"}},
	KDateStrToDate:                       {name: "DateStrToDate", key: "datestrtodate", mro: []Kind{KDateStrToDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DATE_STR_TO_DATE"}},
	KDateToDateStr:                       {name: "DateToDateStr", key: "datetodatestr", mro: []Kind{KDateToDateStr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DATE_TO_DATE_STR"}},
	KDateToDi:                            {name: "DateToDi", key: "datetodi", mro: []Kind{KDateToDi, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DATE_TO_DI"}},
	KDiToDate:                            {name: "DiToDate", key: "ditodate", mro: []Kind{KDiToDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"DI_TO_DATE"}},
	KFromISO8601Timestamp:                {name: "FromISO8601Timestamp", key: "fromiso8601timestamp", mro: []Kind{KFromISO8601Timestamp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"FROM_ISO8601_TIMESTAMP"}},
	KFromISO8601Date:                     {name: "FromISO8601Date", key: "fromiso8601date", mro: []Kind{KFromISO8601Date, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"FROM_ISO8601_DATE"}},
	KFromISO8601TimestampNanos:           {name: "FromISO8601TimestampNanos", key: "fromiso8601timestampnanos", mro: []Kind{KFromISO8601TimestampNanos, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"FROM_ISO8601_TIMESTAMP_NANOS"}},
	KParseDatetime:                       {name: "ParseDatetime", key: "parsedatetime", mro: []Kind{KParseDatetime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}, {"zone", false}, {"default_year", false}}, flags: 0, sqlNames: []string{"PARSE_DATETIME"}},
	KParseTime:                           {name: "ParseTime", key: "parsetime", mro: []Kind{KParseTime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", true}}, flags: 0, sqlNames: []string{"PARSE_TIME"}},
	KStrToDate:                           {name: "StrToDate", key: "strtodate", mro: []Kind{KStrToDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}, {"safe", false}, {"default_year", false}, {"nlsparam", false}}, flags: 0, sqlNames: []string{"STR_TO_DATE"}},
	KStrToTime:                           {name: "StrToTime", key: "strtotime", mro: []Kind{KStrToTime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", true}, {"zone", false}, {"safe", false}, {"target_type", false}, {"default_year", false}, {"nlsparam", false}}, flags: 0, sqlNames: []string{"STR_TO_TIME"}},
	KStrToUnix:                           {name: "StrToUnix", key: "strtounix", mro: []Kind{KStrToUnix, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", false}, {"format", false}}, flags: 0, sqlNames: []string{"STR_TO_UNIX"}},
	KTimeStrToDate:                       {name: "TimeStrToDate", key: "timestrtodate", mro: []Kind{KTimeStrToDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TIME_STR_TO_DATE"}},
	KTimeStrToTime:                       {name: "TimeStrToTime", key: "timestrtotime", mro: []Kind{KTimeStrToTime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"zone", false}}, flags: 0, sqlNames: []string{"TIME_STR_TO_TIME"}},
	KTimeStrToUnix:                       {name: "TimeStrToUnix", key: "timestrtounix", mro: []Kind{KTimeStrToUnix, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TIME_STR_TO_UNIX"}},
	KTimeToStr:                           {name: "TimeToStr", key: "timetostr", mro: []Kind{KTimeToStr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", true}, {"culture", false}, {"zone", false}}, flags: 0, sqlNames: []string{"TIME_TO_STR"}},
	KTimeToTimeStr:                       {name: "TimeToTimeStr", key: "timetotimestr", mro: []Kind{KTimeToTimeStr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TIME_TO_TIME_STR"}},
	KTimeToUnix:                          {name: "TimeToUnix", key: "timetounix", mro: []Kind{KTimeToUnix, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TIME_TO_UNIX"}},
	KTsOrDiToDi:                          {name: "TsOrDiToDi", key: "tsorditodi", mro: []Kind{KTsOrDiToDi, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TS_OR_DI_TO_DI"}},
	KTsOrDsToDate:                        {name: "TsOrDsToDate", key: "tsordstodate", mro: []Kind{KTsOrDsToDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}, {"safe", false}}, flags: 0, sqlNames: []string{"TS_OR_DS_TO_DATE"}},
	KTsOrDsToDateStr:                     {name: "TsOrDsToDateStr", key: "tsordstodatestr", mro: []Kind{KTsOrDsToDateStr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TS_OR_DS_TO_DATE_STR"}},
	KTsOrDsToDatetime:                    {name: "TsOrDsToDatetime", key: "tsordstodatetime", mro: []Kind{KTsOrDsToDatetime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TS_OR_DS_TO_DATETIME"}},
	KTsOrDsToTime:                        {name: "TsOrDsToTime", key: "tsordstotime", mro: []Kind{KTsOrDsToTime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}, {"safe", false}}, flags: 0, sqlNames: []string{"TS_OR_DS_TO_TIME"}},
	KTsOrDsToTimestamp:                   {name: "TsOrDsToTimestamp", key: "tsordstotimestamp", mro: []Kind{KTsOrDsToTimestamp, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"TS_OR_DS_TO_TIMESTAMP"}},
	KUnixDate:                            {name: "UnixDate", key: "unixdate", mro: []Kind{KUnixDate, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"UNIX_DATE"}},
	KUnixMicros:                          {name: "UnixMicros", key: "unixmicros", mro: []Kind{KUnixMicros, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"UNIX_MICROS"}},
	KUnixMillis:                          {name: "UnixMillis", key: "unixmillis", mro: []Kind{KUnixMillis, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"UNIX_MILLIS"}},
	KUnixSeconds:                         {name: "UnixSeconds", key: "unixseconds", mro: []Kind{KUnixSeconds, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"UNIX_SECONDS"}},
	KUnixToStr:                           {name: "UnixToStr", key: "unixtostr", mro: []Kind{KUnixToStr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"format", false}}, flags: 0, sqlNames: []string{"UNIX_TO_STR"}},
	KUnixToTime:                          {name: "UnixToTime", key: "unixtotime", mro: []Kind{KUnixToTime, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"scale", false}, {"zone", false}, {"hours", false}, {"minutes", false}, {"format", false}, {"target_type", false}}, flags: 0, sqlNames: []string{"UNIX_TO_TIME"}},
	KUnixToTimeStr:                       {name: "UnixToTimeStr", key: "unixtotimestr", mro: []Kind{KUnixToTimeStr, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{"UNIX_TO_TIME_STR"}},
	KNearest:                             {name: "Nearest", key: "nearest", mro: []Kind{KNearest, KExpression, KExpr}, args: []argSpec{{"this", true}, {"count", true}, {"approximate", false}, {"similarity", false}}, flags: 0, sqlNames: []string{}},
	KConnectTo:                           {name: "ConnectTo", key: "connectto", mro: []Kind{KConnectTo, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KDisconnect:                          {name: "Disconnect", key: "disconnect", mro: []Kind{KDisconnect, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KPos:                                 {name: "Pos", key: "pos", mro: []Kind{KPos, KUnary, KExpression, KCondition, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KFunctionSpecification:               {name: "FunctionSpecification", key: "functionspecification", mro: []Kind{KFunctionSpecification, KExpression, KExpr}, args: []argSpec{{"this", true}, {"characteristics", false}, {"properties", false}, {"expression", true}}, flags: 0, sqlNames: []string{}},
	KJSONBContainsTopKey:                 {name: "JSONBContainsTopKey", key: "jsonbcontainstopkey", mro: []Kind{KJSONBContainsTopKey, KExpression, KBinary, KPredicate, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"expression", true}}, flags: 0, sqlNames: []string{"J_S_O_N_B_CONTAINS_TOP_KEY"}},
	KPartitionByExcluding:                {name: "PartitionByExcluding", key: "partitionbyexcluding", mro: []Kind{KPartitionByExcluding, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KOptimize:                            {name: "Optimize", key: "optimize", mro: []Kind{KOptimize, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KTableSpaceProperty:                  {name: "TableSpaceProperty", key: "tablespaceproperty", mro: []Kind{KTableSpaceProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KPer:                                 {name: "Per", key: "per", mro: []Kind{KPer, KExpression, KExpr}, args: []argSpec{{"this", true}, {"expressions", true}}, flags: 0, sqlNames: []string{}},
	KPhysicalProperty:                    {name: "PhysicalProperty", key: "physicalproperty", mro: []Kind{KPhysicalProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"this", true}, {"value", false}}, flags: 0, sqlNames: []string{}},
	KStorageProperty:                     {name: "StorageProperty", key: "storageproperty", mro: []Kind{KStorageProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KListStage:                           {name: "ListStage", key: "liststage", mro: []Kind{KListStage, KExpression, KExpr}, args: []argSpec{{"this", true}, {"pattern", false}}, flags: 0, sqlNames: []string{}},
	KBinaryColumnConstraint:              {name: "BinaryColumnConstraint", key: "binarycolumnconstraint", mro: []Kind{KBinaryColumnConstraint, KColumnConstraint, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KWeightString:                        {name: "WeightString", key: "weightstring", mro: []Kind{KWeightString, KExpression, KFunc, KCondition, KExpr}, args: []argSpec{{"this", true}, {"to", false}}, flags: 0, sqlNames: []string{"WEIGHT_STRING"}},
	KCopyTagsProperty:                    {name: "CopyTagsProperty", key: "copytagsproperty", mro: []Kind{KCopyTagsProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KReplaceColumns:                      {name: "ReplaceColumns", key: "replacecolumns", mro: []Kind{KReplaceColumns, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KWithoutRowidProperty:                {name: "WithoutRowidProperty", key: "withoutrowidproperty", mro: []Kind{KWithoutRowidProperty, KProperty, KExpression, KExpr}, args: []argSpec{}, flags: 0, sqlNames: []string{}},
	KAggregateKeyProperty:                {name: "AggregateKeyProperty", key: "aggregatekeyproperty", mro: []Kind{KAggregateKeyProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
	KCaseStatement:                       {name: "CaseStatement", key: "casestatement", mro: []Kind{KCaseStatement, KExpression, KExpr}, args: []argSpec{{"this", false}, {"ifs", true}, {"default", false}}, flags: 0, sqlNames: []string{}},
	KLoopBlock:                           {name: "LoopBlock", key: "loopblock", mro: []Kind{KLoopBlock, KExpression, KExpr}, args: []argSpec{{"body", true}, {"label", false}}, flags: 0, sqlNames: []string{}},
	KRepeatBlock:                         {name: "RepeatBlock", key: "repeatblock", mro: []Kind{KRepeatBlock, KExpression, KExpr}, args: []argSpec{{"body", true}, {"until", true}, {"label", false}}, flags: 0, sqlNames: []string{}},
	KLeave:                               {name: "Leave", key: "leave", mro: []Kind{KLeave, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KIterate:                             {name: "Iterate", key: "iterate", mro: []Kind{KIterate, KExpression, KExpr}, args: []argSpec{{"this", true}}, flags: 0, sqlNames: []string{}},
	KOptionsProperty:                     {name: "OptionsProperty", key: "optionsproperty", mro: []Kind{KOptionsProperty, KProperty, KExpression, KExpr}, args: []argSpec{{"expressions", true}}, flags: 0, sqlNames: []string{}},
}

var kindByKey = map[string]Kind{
	"aiagg":                               KAIAgg,
	"aisummarizeagg":                      KAISummarizeAgg,
	"anyvalue":                            KAnyValue,
	"approximatesimilarity":               KApproximateSimilarity,
	"approxpercentileaccumulate":          KApproxPercentileAccumulate,
	"approxpercentilecombine":             KApproxPercentileCombine,
	"approxpercentileestimate":            KApproxPercentileEstimate,
	"approxquantiles":                     KApproxQuantiles,
	"approxtopk":                          KApproxTopK,
	"approxtopkaccumulate":                KApproxTopKAccumulate,
	"approxtopkcombine":                   KApproxTopKCombine,
	"approxtopkestimate":                  KApproxTopKEstimate,
	"approxtopsum":                        KApproxTopSum,
	"argmax":                              KArgMax,
	"argmin":                              KArgMin,
	"arrayagg":                            KArrayAgg,
	"arrayconcatagg":                      KArrayConcatAgg,
	"arrayunionagg":                       KArrayUnionAgg,
	"arrayuniqueagg":                      KArrayUniqueAgg,
	"avg":                                 KAvg,
	"corr":                                KCorr,
	"count":                               KCount,
	"countif":                             KCountIf,
	"covarpop":                            KCovarPop,
	"covarsamp":                           KCovarSamp,
	"cumedist":                            KCumeDist,
	"denserank":                           KDenseRank,
	"first":                               KFirst,
	"firstvalue":                          KFirstValue,
	"groupconcat":                         KGroupConcat,
	"grouping":                            KGrouping,
	"groupingid":                          KGroupingId,
	"kurtosis":                            KKurtosis,
	"lag":                                 KLag,
	"last":                                KLast,
	"lastvalue":                           KLastValue,
	"lead":                                KLead,
	"logicaland":                          KLogicalAnd,
	"logicalor":                           KLogicalOr,
	"max":                                 KMax,
	"median":                              KMedian,
	"min":                                 KMin,
	"minhash":                             KMinhash,
	"minhashcombine":                      KMinhashCombine,
	"mode":                                KMode,
	"ntile":                               KNtile,
	"nthvalue":                            KNthValue,
	"objectagg":                           KObjectAgg,
	"percentilecont":                      KPercentileCont,
	"percentiledisc":                      KPercentileDisc,
	"percentrank":                         KPercentRank,
	"quantile":                            KQuantile,
	"approxquantile":                      KApproxQuantile,
	"rank":                                KRank,
	"regravgx":                            KRegrAvgx,
	"regravgy":                            KRegrAvgy,
	"regrcount":                           KRegrCount,
	"regrintercept":                       KRegrIntercept,
	"regrr2":                              KRegrR2,
	"regrslope":                           KRegrSlope,
	"regrsxx":                             KRegrSxx,
	"regrsxy":                             KRegrSxy,
	"regrsyy":                             KRegrSyy,
	"regrvalx":                            KRegrValx,
	"regrvaly":                            KRegrValy,
	"rownumber":                           KRowNumber,
	"skewness":                            KSkewness,
	"stddev":                              KStddev,
	"stddevpop":                           KStddevPop,
	"stddevsamp":                          KStddevSamp,
	"sum":                                 KSum,
	"variance":                            KVariance,
	"variancepop":                         KVariancePop,
	"array":                               KArray,
	"arrayconstructcompact":               KArrayConstructCompact,
	"list":                                KList,
	"toarray":                             KToArray,
	"arrayappend":                         KArrayAppend,
	"arraycompact":                        KArrayCompact,
	"arrayconcat":                         KArrayConcat,
	"arrayfilter":                         KArrayFilter,
	"arrayinsert":                         KArrayInsert,
	"arrayprepend":                        KArrayPrepend,
	"arrayremove":                         KArrayRemove,
	"arrayremoveat":                       KArrayRemoveAt,
	"arrayreverse":                        KArrayReverse,
	"arrayslice":                          KArraySlice,
	"arraysort":                           KArraySort,
	"sortarray":                           KSortArray,
	"arrayall":                            KArrayAll,
	"arrayany":                            KArrayAny,
	"arraycontains":                       KArrayContains,
	"arraycontainsall":                    KArrayContainsAll,
	"arraycontainedby":                    KArrayContainedBy,
	"arrayexcept":                         KArrayExcept,
	"arrayintersect":                      KArrayIntersect,
	"arrayoverlaps":                       KArrayOverlaps,
	"arrayposition":                       KArrayPosition,
	"arraydistinct":                       KArrayDistinct,
	"arrayfirst":                          KArrayFirst,
	"arraylast":                           KArrayLast,
	"arraymax":                            KArrayMax,
	"arraymin":                            KArrayMin,
	"arraysize":                           KArraySize,
	"arraysum":                            KArraySum,
	"arrayszip":                           KArraysZip,
	"arraytostring":                       KArrayToString,
	"flatten":                             KFlatten,
	"stringtoarray":                       KStringToArray,
	"strtoktoarray":                       KStrtokToArray,
	"apply":                               KApply,
	"reduce":                              KReduce,
	"transform":                           KTransform,
	"generateseries":                      KGenerateSeries,
	"explodinggenerateseries":             KExplodingGenerateSeries,
	"generator":                           KGenerator,
	"explode":                             KExplode,
	"inline":                              KInline,
	"explodeouter":                        KExplodeOuter,
	"_explodeouter":                       K_ExplodeOuter,
	"posexplode":                          KPosexplode,
	"posexplodeouter":                     KPosexplodeOuter,
	"positionalcolumn":                    KPositionalColumn,
	"unnest":                              KUnnest,
	"map":                                 KMap,
	"mapcat":                              KMapCat,
	"mapcontainskey":                      KMapContainsKey,
	"mapdelete":                           KMapDelete,
	"mapfromentries":                      KMapFromEntries,
	"mapinsert":                           KMapInsert,
	"mapkeys":                             KMapKeys,
	"mappick":                             KMapPick,
	"mapsize":                             KMapSize,
	"starmap":                             KStarMap,
	"tomap":                               KToMap,
	"varmap":                              KVarMap,
	"struct":                              KStruct,
	"structextract":                       KStructExtract,
	"stdistance":                          KStDistance,
	"stpoint":                             KStPoint,
	"indexconstraintoption":               KIndexConstraintOption,
	"reference":                           KReference,
	"columnconstraint":                    KColumnConstraint,
	"autoincrementcolumnconstraint":       KAutoIncrementColumnConstraint,
	"invisiblecolumnconstraint":           KInvisibleColumnConstraint,
	"zerofillcolumnconstraint":            KZeroFillColumnConstraint,
	"periodforsystemtimeconstraint":       KPeriodForSystemTimeConstraint,
	"casespecificcolumnconstraint":        KCaseSpecificColumnConstraint,
	"charactersetcolumnconstraint":        KCharacterSetColumnConstraint,
	"checkcolumnconstraint":               KCheckColumnConstraint,
	"assumecolumnconstraint":              KAssumeColumnConstraint,
	"clusteredcolumnconstraint":           KClusteredColumnConstraint,
	"collatecolumnconstraint":             KCollateColumnConstraint,
	"commentcolumnconstraint":             KCommentColumnConstraint,
	"compresscolumnconstraint":            KCompressColumnConstraint,
	"dateformatcolumnconstraint":          KDateFormatColumnConstraint,
	"defaultcolumnconstraint":             KDefaultColumnConstraint,
	"encodecolumnconstraint":              KEncodeColumnConstraint,
	"excludecolumnconstraint":             KExcludeColumnConstraint,
	"ephemeralcolumnconstraint":           KEphemeralColumnConstraint,
	"withoperator":                        KWithOperator,
	"generatedasidentitycolumnconstraint": KGeneratedAsIdentityColumnConstraint,
	"generatedasrowcolumnconstraint":      KGeneratedAsRowColumnConstraint,
	"indexcolumnconstraint":               KIndexColumnConstraint,
	"inlinelengthcolumnconstraint":        KInlineLengthColumnConstraint,
	"nonclusteredcolumnconstraint":        KNonClusteredColumnConstraint,
	"notforreplicationcolumnconstraint":   KNotForReplicationColumnConstraint,
	"maskingpolicycolumnconstraint":       KMaskingPolicyColumnConstraint,
	"notnullcolumnconstraint":             KNotNullColumnConstraint,
	"onupdatecolumnconstraint":            KOnUpdateColumnConstraint,
	"primarykeycolumnconstraint":          KPrimaryKeyColumnConstraint,
	"titlecolumnconstraint":               KTitleColumnConstraint,
	"uniquecolumnconstraint":              KUniqueColumnConstraint,
	"uppercasecolumnconstraint":           KUppercaseColumnConstraint,
	"watermarkcolumnconstraint":           KWatermarkColumnConstraint,
	"pathcolumnconstraint":                KPathColumnConstraint,
	"projectionpolicycolumnconstraint":    KProjectionPolicyColumnConstraint,
	"computedcolumnconstraint":            KComputedColumnConstraint,
	"inoutcolumnconstraint":               KInOutColumnConstraint,
	"constraint":                          KConstraint,
	"foreignkey":                          KForeignKey,
	"columnprefix":                        KColumnPrefix,
	"primarykey":                          KPrimaryKey,
	"timeserieskey":                       KTimeseriesKey,
	"indexparameters":                     KIndexParameters,
	"addconstraint":                       KAddConstraint,
	"expression":                          KExpression,
	"condition":                           KCondition,
	"predicate":                           KPredicate,
	"cache":                               KCache,
	"uncache":                             KUncache,
	"refresh":                             KRefresh,
	"lockingstatement":                    KLockingStatement,
	"columnconstraintkind":                KColumnConstraintKind,
	"subquerypredicate":                   KSubqueryPredicate,
	"all":                                 KAll,
	"any":                                 KAny,
	"binary":                              KBinary,
	"connector":                           KConnector,
	"func":                                KFunc,
	"aggfunc":                             KAggFunc,
	"column":                              KColumn,
	"literal":                             KLiteral,
	"var":                                 KVar,
	"withingroup":                         KWithinGroup,
	"pseudocolumn":                        KPseudocolumn,
	"hint":                                KHint,
	"joinhint":                            KJoinHint,
	"identifier":                          KIdentifier,
	"dynamicidentifier":                   KDynamicIdentifier,
	"opclass":                             KOpclass,
	"star":                                KStar,
	"parameter":                           KParameter,
	"sessionparameter":                    KSessionParameter,
	"placeholder":                         KPlaceholder,
	"null":                                KNull,
	"boolean":                             KBoolean,
	"dot":                                 KDot,
	"kwarg":                               KKwarg,
	"alias":                               KAlias,
	"pivotalias":                          KPivotAlias,
	"pivotany":                            KPivotAny,
	"aliases":                             KAliases,
	"bracket":                             KBracket,
	"forin":                               KForIn,
	"ignorenulls":                         KIgnoreNulls,
	"respectnulls":                        KRespectNulls,
	"havingmax":                           KHavingMax,
	"safefunc":                            KSafeFunc,
	"typeof":                              KTypeof,
	"parameterizedagg":                    KParameterizedAgg,
	"anonymous":                           KAnonymous,
	"anonymousaggfunc":                    KAnonymousAggFunc,
	"combinedaggfunc":                     KCombinedAggFunc,
	"combinedparameterizedagg":            KCombinedParameterizedAgg,
	"hashagg":                             KHashAgg,
	"hll":                                 KHll,
	"approxdistinct":                      KApproxDistinct,
	"slice":                               KSlice,
	"timeunit":                            KTimeUnit,
	"_timeunit":                           K_TimeUnit,
	"intervalop":                          KIntervalOp,
	"filter":                              KFilter,
	"check":                               KCheck,
	"ordered":                             KOrdered,
	"add":                                 KAdd,
	"bitwiseand":                          KBitwiseAnd,
	"bitwiseleftshift":                    KBitwiseLeftShift,
	"bitwiseor":                           KBitwiseOr,
	"bitwiserightshift":                   KBitwiseRightShift,
	"bitwisexor":                          KBitwiseXor,
	"div":                                 KDiv,
	"overlaps":                            KOverlaps,
	"extendsleft":                         KExtendsLeft,
	"extendsright":                        KExtendsRight,
	"dpipe":                               KDPipe,
	"eq":                                  KEQ,
	"nullsafeeq":                          KNullSafeEQ,
	"nullsafeneq":                         KNullSafeNEQ,
	"propertyeq":                          KPropertyEQ,
	"distance":                            KDistance,
	"distancend":                          KDistanceNd,
	"escape":                              KEscape,
	"glob":                                KGlob,
	"gt":                                  KGT,
	"gte":                                 KGTE,
	"ilike":                               KILike,
	"intdiv":                              KIntDiv,
	"is":                                  KIs,
	"like":                                KLike,
	"match":                               KMatch,
	"lt":                                  KLT,
	"lte":                                 KLTE,
	"mod":                                 KMod,
	"mul":                                 KMul,
	"neq":                                 KNEQ,
	"nestedjsonselect":                    KNestedJSONSelect,
	"operator":                            KOperator,
	"similarto":                           KSimilarTo,
	"sub":                                 KSub,
	"adjacent":                            KAdjacent,
	"unary":                               KUnary,
	"bitwisenot":                          KBitwiseNot,
	"not":                                 KNot,
	"paren":                               KParen,
	"neg":                                 KNeg,
	"atindex":                             KAtIndex,
	"attimezone":                          KAtTimeZone,
	"fromtimezone":                        KFromTimeZone,
	"formatphrase":                        KFormatPhrase,
	"between":                             KBetween,
	"distinct":                            KDistinct,
	"in":                                  KIn,
	"and":                                 KAnd,
	"or":                                  KOr,
	"xor":                                 KXor,
	"pow":                                 KPow,
	"regexplike":                          KRegexpLike,
	"datatypeparam":                       KDataTypeParam,
	"datatype":                            KDataType,
	"pseudotype":                          KPseudoType,
	"objectidentifier":                    KObjectIdentifier,
	"intervalspan":                        KIntervalSpan,
	"interval":                            KInterval,
	"ddl":                                 KDDL,
	"create":                              KCreate,
	"sequenceproperties":                  KSequenceProperties,
	"triggerproperties":                   KTriggerProperties,
	"triggerexecute":                      KTriggerExecute,
	"triggerevent":                        KTriggerEvent,
	"triggerreferencing":                  KTriggerReferencing,
	"truncatetable":                       KTruncateTable,
	"clone":                               KClone,
	"describe":                            KDescribe,
	"attach":                              KAttach,
	"detach":                              KDetach,
	"install":                             KInstall,
	"summarize":                           KSummarize,
	"kill":                                KKill,
	"pragma":                              KPragma,
	"declare":                             KDeclare,
	"declareitem":                         KDeclareItem,
	"set":                                 KSet,
	"heredoc":                             KHeredoc,
	"setitem":                             KSetItem,
	"show":                                KShow,
	"userdefinedfunction":                 KUserDefinedFunction,
	"macrooverloads":                      KMacroOverloads,
	"macrooverload":                       KMacroOverload,
	"characterset":                        KCharacterSet,
	"altercolumn":                         KAlterColumn,
	"modifycolumn":                        KModifyColumn,
	"alterindex":                          KAlterIndex,
	"alterdiststyle":                      KAlterDistStyle,
	"altersortkey":                        KAlterSortKey,
	"alterset":                            KAlterSet,
	"renamecolumn":                        KRenameColumn,
	"alterrename":                         KAlterRename,
	"renameindex":                         KRenameIndex,
	"altermodifysqlsecurity":              KAlterModifySqlSecurity,
	"swaptable":                           KSwapTable,
	"comment":                             KComment,
	"comprehension":                       KComprehension,
	"mergetreettlaction":                  KMergeTreeTTLAction,
	"mergetreettl":                        KMergeTreeTTL,
	"drop":                                KDrop,
	"dropprimarykey":                      KDropPrimaryKey,
	"undrop":                              KUndrop,
	"command":                             KCommand,
	"transaction":                         KTransaction,
	"commit":                              KCommit,
	"rollback":                            KRollback,
	"alter":                               KAlter,
	"altersession":                        KAlterSession,
	"use":                                 KUse,
	"nextvaluefor":                        KNextValueFor,
	"execute":                             KExecute,
	"executesql":                          KExecuteSql,
	"dml":                                 KDML,
	"delete":                              KDelete,
	"export":                              KExport,
	"copyparameter":                       KCopyParameter,
	"copy":                                KCopy,
	"credentials":                         KCredentials,
	"directory":                           KDirectory,
	"directorystage":                      KDirectoryStage,
	"insert":                              KInsert,
	"onconflict":                          KOnConflict,
	"returning":                           KReturning,
	"loaddata":                            KLoadData,
	"update":                              KUpdate,
	"merge":                               KMerge,
	"when":                                KWhen,
	"whens":                               KWhens,
	"cast":                                KCast,
	"trycast":                             KTryCast,
	"jsoncast":                            KJSONCast,
	"casttostrtype":                       KCastToStrType,
	"convert":                             KConvert,
	"if":                                  KIf,
	"case":                                KCase,
	"coalesce":                            KCoalesce,
	"decodecase":                          KDecodeCase,
	"equalnull":                           KEqualNull,
	"greatest":                            KGreatest,
	"least":                               KLeast,
	"nullif":                              KNullif,
	"objecttransform":                     KObjectTransform,
	"nvl2":                                KNvl2,
	"try":                                 KTry,
	"collate":                             KCollate,
	"collation":                           KCollation,
	"connectbyroot":                       KConnectByRoot,
	"checkxml":                            KCheckXml,
	"exists":                              KExists,
	"float64":                             KFloat64,
	"int64":                               KInt64,
	"isarray":                             KIsArray,
	"isnullvalue":                         KIsNullValue,
	"laxbool":                             KLaxBool,
	"laxfloat64":                          KLaxFloat64,
	"laxint64":                            KLaxInt64,
	"laxstring":                           KLaxString,
	"toboolean":                           KToBoolean,
	"tovariant":                           KToVariant,
	"currentaccount":                      KCurrentAccount,
	"currentaccountname":                  KCurrentAccountName,
	"currentavailableroles":               KCurrentAvailableRoles,
	"currentcatalog":                      KCurrentCatalog,
	"currentclient":                       KCurrentClient,
	"currentdatabase":                     KCurrentDatabase,
	"currentipaddress":                    KCurrentIpAddress,
	"currentorganizationname":             KCurrentOrganizationName,
	"currentorganizationuser":             KCurrentOrganizationUser,
	"currentregion":                       KCurrentRegion,
	"currentrole":                         KCurrentRole,
	"currentroletype":                     KCurrentRoleType,
	"currentschema":                       KCurrentSchema,
	"currentschemas":                      KCurrentSchemas,
	"currentsecondaryroles":               KCurrentSecondaryRoles,
	"currentsession":                      KCurrentSession,
	"currentstatement":                    KCurrentStatement,
	"currenttransaction":                  KCurrentTransaction,
	"currentuser":                         KCurrentUser,
	"currentuserid":                       KCurrentUserId,
	"currentversion":                      KCurrentVersion,
	"currentwarehouse":                    KCurrentWarehouse,
	"sessionuser":                         KSessionUser,
	"aiclassify":                          KAIClassify,
	"aiembed":                             KAIEmbed,
	"aisimilarity":                        KAISimilarity,
	"aigenerate":                          KAIGenerate,
	"featuresattime":                      KFeaturesAtTime,
	"generateembedding":                   KGenerateEmbedding,
	"generatetext":                        KGenerateText,
	"generatetable":                       KGenerateTable,
	"generatebool":                        KGenerateBool,
	"generateint":                         KGenerateInt,
	"generatedouble":                      KGenerateDouble,
	"mlforecast":                          KMLForecast,
	"aiforecast":                          KAIForecast,
	"mltranslate":                         KMLTranslate,
	"predict":                             KPredict,
	"vectorsearch":                        KVectorSearch,
	"readcsv":                             KReadCSV,
	"readparquet":                         KReadParquet,
	"xmlelement":                          KXMLElement,
	"getignorecase":                       KGetIgnoreCase,
	"xmlget":                              KXMLGet,
	"xmltable":                            KXMLTable,
	"host":                                KHost,
	"netfunc":                             KNetFunc,
	"parseip":                             KParseIp,
	"regdomain":                           KRegDomain,
	"columns":                             KColumns,
	"normal":                              KNormal,
	"rand":                                KRand,
	"randn":                               KRandn,
	"randstr":                             KRandstr,
	"rangebucket":                         KRangeBucket,
	"rangen":                              KRangeN,
	"seq1":                                KSeq1,
	"seq2":                                KSeq2,
	"seq4":                                KSeq4,
	"seq8":                                KSeq8,
	"uniform":                             KUniform,
	"uuid":                                KUuid,
	"weekstart":                           KWeekStart,
	"widthbucket":                         KWidthBucket,
	"zipf":                                KZipf,
	"checkjson":                           KCheckJson,
	"jsonarray":                           KJSONArray,
	"jsonarrayagg":                        KJSONArrayAgg,
	"jsonarrayappend":                     KJSONArrayAppend,
	"jsonarraycontains":                   KJSONArrayContains,
	"jsonarrayinsert":                     KJSONArrayInsert,
	"jsonbcontains":                       KJSONBContains,
	"jsonbcontainsalltopkeys":             KJSONBContainsAllTopKeys,
	"jsonbcontainsanytopkeys":             KJSONBContainsAnyTopKeys,
	"jsonbdeleteatpath":                   KJSONBDeleteAtPath,
	"jsonbpathexists":                     KJSONBPathExists,
	"jsonbexists":                         KJSONBExists,
	"jsonbextract":                        KJSONBExtract,
	"jsonbextractscalar":                  KJSONBExtractScalar,
	"jsonbobjectagg":                      KJSONBObjectAgg,
	"jsonbool":                            KJSONBool,
	"jsonexists":                          KJSONExists,
	"jsonextract":                         KJSONExtract,
	"jsonextractarray":                    KJSONExtractArray,
	"jsonextractscalar":                   KJSONExtractScalar,
	"jsonformat":                          KJSONFormat,
	"jsonkeys":                            KJSONKeys,
	"jsonkeysatdepth":                     KJSONKeysAtDepth,
	"jsonobject":                          KJSONObject,
	"jsonobjectagg":                       KJSONObjectAgg,
	"jsonremove":                          KJSONRemove,
	"jsonset":                             KJSONSet,
	"jsonstripnulls":                      KJSONStripNulls,
	"stripnullvalue":                      KStripNullValue,
	"jsontable":                           KJSONTable,
	"jsontype":                            KJSONType,
	"objectid":                            KObjectId,
	"objectinsert":                        KObjectInsert,
	"openjson":                            KOpenJSON,
	"parsejson":                           KParseJSON,
	"acos":                                KAcos,
	"acosh":                               KAcosh,
	"asin":                                KAsin,
	"asinh":                               KAsinh,
	"atan":                                KAtan,
	"atanh":                               KAtanh,
	"atan2":                               KAtan2,
	"cos":                                 KCos,
	"cosh":                                KCosh,
	"cot":                                 KCot,
	"coth":                                KCoth,
	"csc":                                 KCsc,
	"csch":                                KCsch,
	"degrees":                             KDegrees,
	"radians":                             KRadians,
	"sec":                                 KSec,
	"sech":                                KSech,
	"sin":                                 KSin,
	"sinh":                                KSinh,
	"tan":                                 KTan,
	"tanh":                                KTanh,
	"cosinedistance":                      KCosineDistance,
	"dotproduct":                          KDotProduct,
	"euclideandistance":                   KEuclideanDistance,
	"jarowinklersimilarity":               KJarowinklerSimilarity,
	"manhattandistance":                   KManhattanDistance,
	"abs":                                 KAbs,
	"cbrt":                                KCbrt,
	"ceil":                                KCeil,
	"exp":                                 KExp,
	"factorial":                           KFactorial,
	"floor":                               KFloor,
	"isinf":                               KIsInf,
	"isnan":                               KIsNan,
	"ln":                                  KLn,
	"log":                                 KLog,
	"pi":                                  KPi,
	"rint":                                KRint,
	"round":                               KRound,
	"sign":                                KSign,
	"sqrt":                                KSqrt,
	"trunc":                               KTrunc,
	"safeadd":                             KSafeAdd,
	"safedivide":                          KSafeDivide,
	"safemultiply":                        KSafeMultiply,
	"safenegate":                          KSafeNegate,
	"safesubtract":                        KSafeSubtract,
	"bitwiseandagg":                       KBitwiseAndAgg,
	"bitwisecount":                        KBitwiseCount,
	"bitwiseoragg":                        KBitwiseOrAgg,
	"bitwisexoragg":                       KBitwiseXorAgg,
	"bitmapbitposition":                   KBitmapBitPosition,
	"bitmapbucketnumber":                  KBitmapBucketNumber,
	"bitmapconstructagg":                  KBitmapConstructAgg,
	"bitmapcount":                         KBitmapCount,
	"bitmaporagg":                         KBitmapOrAgg,
	"booland":                             KBooland,
	"boolnot":                             KBoolnot,
	"boolor":                              KBoolor,
	"boolxoragg":                          KBoolxorAgg,
	"getbit":                              KGetbit,
	"property":                            KProperty,
	"grantprivilege":                      KGrantPrivilege,
	"grantprincipal":                      KGrantPrincipal,
	"allowedvaluesproperty":               KAllowedValuesProperty,
	"algorithmproperty":                   KAlgorithmProperty,
	"apiproperty":                         KApiProperty,
	"applicationproperty":                 KApplicationProperty,
	"autoincrementproperty":               KAutoIncrementProperty,
	"autorefreshproperty":                 KAutoRefreshProperty,
	"backupproperty":                      KBackupProperty,
	"buildproperty":                       KBuildProperty,
	"blockcompressionproperty":            KBlockCompressionProperty,
	"calledonnullinputproperty":           KCalledOnNullInputProperty,
	"catalogproperty":                     KCatalogProperty,
	"charactersetproperty":                KCharacterSetProperty,
	"checksumproperty":                    KChecksumProperty,
	"clusterproperty":                     KClusterProperty,
	"collateproperty":                     KCollateProperty,
	"computeproperty":                     KComputeProperty,
	"copygrantsproperty":                  KCopyGrantsProperty,
	"datablocksizeproperty":               KDataBlocksizeProperty,
	"datadeletionproperty":                KDataDeletionProperty,
	"databaseproperty":                    KDatabaseProperty,
	"definerproperty":                     KDefinerProperty,
	"distkeyproperty":                     KDistKeyProperty,
	"distributedbyproperty":               KDistributedByProperty,
	"diststyleproperty":                   KDistStyleProperty,
	"duplicatekeyproperty":                KDuplicateKeyProperty,
	"engineproperty":                      KEngineProperty,
	"uuidproperty":                        KUuidProperty,
	"heapproperty":                        KHeapProperty,
	"hybridproperty":                      KHybridProperty,
	"handlerproperty":                     KHandlerProperty,
	"parameterstyleproperty":              KParameterStyleProperty,
	"totableproperty":                     KToTableProperty,
	"executeasproperty":                   KExecuteAsProperty,
	"externalproperty":                    KExternalProperty,
	"fallbackproperty":                    KFallbackProperty,
	"fileformatproperty":                  KFileFormatProperty,
	"credentialsproperty":                 KCredentialsProperty,
	"freespaceproperty":                   KFreespaceProperty,
	"globalproperty":                      KGlobalProperty,
	"icebergproperty":                     KIcebergProperty,
	"inheritsproperty":                    KInheritsProperty,
	"inputmodelproperty":                  KInputModelProperty,
	"outputmodelproperty":                 KOutputModelProperty,
	"isolatedloadingproperty":             KIsolatedLoadingProperty,
	"journalproperty":                     KJournalProperty,
	"languageproperty":                    KLanguageProperty,
	"enviromentproperty":                  KEnviromentProperty,
	"clusteredbyproperty":                 KClusteredByProperty,
	"dictproperty":                        KDictProperty,
	"dictsubproperty":                     KDictSubProperty,
	"dictrange":                           KDictRange,
	"dynamicproperty":                     KDynamicProperty,
	"oncluster":                           KOnCluster,
	"emptyproperty":                       KEmptyProperty,
	"likeproperty":                        KLikeProperty,
	"locationproperty":                    KLocationProperty,
	"lockproperty":                        KLockProperty,
	"lockingproperty":                     KLockingProperty,
	"logproperty":                         KLogProperty,
	"maskingproperty":                     KMaskingProperty,
	"materializedproperty":                KMaterializedProperty,
	"mergeblockratioproperty":             KMergeBlockRatioProperty,
	"moduleproperty":                      KModuleProperty,
	"networkproperty":                     KNetworkProperty,
	"noprimaryindexproperty":              KNoPrimaryIndexProperty,
	"onproperty":                          KOnProperty,
	"oncommitproperty":                    KOnCommitProperty,
	"partitionedbyproperty":               KPartitionedByProperty,
	"partitionedbybucket":                 KPartitionedByBucket,
	"partitionbytruncate":                 KPartitionByTruncate,
	"partitionbyrangeproperty":            KPartitionByRangeProperty,
	"partitionbyrangepropertydynamic":     KPartitionByRangePropertyDynamic,
	"rollupproperty":                      KRollupProperty,
	"rollupindex":                         KRollupIndex,
	"rowaccessproperty":                   KRowAccessProperty,
	"partitionbylistproperty":             KPartitionByListProperty,
	"partitionlist":                       KPartitionList,
	"refreshtriggerproperty":              KRefreshTriggerProperty,
	"uniquekeyproperty":                   KUniqueKeyProperty,
	"partitionboundspec":                  KPartitionBoundSpec,
	"partitionedofproperty":               KPartitionedOfProperty,
	"streamingtableproperty":              KStreamingTableProperty,
	"remotewithconnectionmodelproperty":   KRemoteWithConnectionModelProperty,
	"returnsproperty":                     KReturnsProperty,
	"strictproperty":                      KStrictProperty,
	"rowformatproperty":                   KRowFormatProperty,
	"rowformatdelimitedproperty":          KRowFormatDelimitedProperty,
	"rowformatserdeproperty":              KRowFormatSerdeProperty,
	"querytransform":                      KQueryTransform,
	"sampleproperty":                      KSampleProperty,
	"schemacommentproperty":               KSchemaCommentProperty,
	"semanticview":                        KSemanticView,
	"serdeproperties":                     KSerdeProperties,
	"setproperty":                         KSetProperty,
	"sharingproperty":                     KSharingProperty,
	"setconfigproperty":                   KSetConfigProperty,
	"settingsproperty":                    KSettingsProperty,
	"sortkeyproperty":                     KSortKeyProperty,
	"sqlreadwriteproperty":                KSqlReadWriteProperty,
	"sqlsecurityproperty":                 KSqlSecurityProperty,
	"stabilityproperty":                   KStabilityProperty,
	"storagehandlerproperty":              KStorageHandlerProperty,
	"usingproperty":                       KUsingProperty,
	"temporaryproperty":                   KTemporaryProperty,
	"virtualproperty":                     KVirtualProperty,
	"secureproperty":                      KSecureProperty,
	"securityintegrationproperty":         KSecurityIntegrationProperty,
	"tags":                                KTags,
	"transformmodelproperty":              KTransformModelProperty,
	"transientproperty":                   KTransientProperty,
	"unloggedproperty":                    KUnloggedProperty,
	"usingtemplateproperty":               KUsingTemplateProperty,
	"viewattributeproperty":               KViewAttributeProperty,
	"volatileproperty":                    KVolatileProperty,
	"withdataproperty":                    KWithDataProperty,
	"withjournaltableproperty":            KWithJournalTableProperty,
	"withschemabindingproperty":           KWithSchemaBindingProperty,
	"withsystemversioningproperty":        KWithSystemVersioningProperty,
	"withprocedureoptions":                KWithProcedureOptions,
	"encodeproperty":                      KEncodeProperty,
	"includeproperty":                     KIncludeProperty,
	"forceproperty":                       KForceProperty,
	"properties":                          KProperties,
	"selectable":                          KSelectable,
	"derivedtable":                        KDerivedTable,
	"udtf":                                KUDTF,
	"query":                               KQuery,
	"queryband":                           KQueryBand,
	"recursivewithsearch":                 KRecursiveWithSearch,
	"with":                                KWith,
	"cte":                                 KCTE,
	"projectiondef":                       KProjectionDef,
	"tablealias":                          KTableAlias,
	"bitstring":                           KBitString,
	"hexstring":                           KHexString,
	"bytestring":                          KByteString,
	"rawstring":                           KRawString,
	"unicodestring":                       KUnicodeString,
	"columnposition":                      KColumnPosition,
	"columndef":                           KColumnDef,
	"changes":                             KChanges,
	"connect":                             KConnect,
	"prior":                               KPrior,
	"into":                                KInto,
	"from":                                KFrom,
	"having":                              KHaving,
	"index":                               KIndex,
	"conditionalinsert":                   KConditionalInsert,
	"multitableinserts":                   KMultitableInserts,
	"oncondition":                         KOnCondition,
	"introducer":                          KIntroducer,
	"national":                            KNational,
	"partition":                           KPartition,
	"partitionrange":                      KPartitionRange,
	"partitionid":                         KPartitionId,
	"fetch":                               KFetch,
	"grant":                               KGrant,
	"revoke":                              KRevoke,
	"group":                               KGroup,
	"cube":                                KCube,
	"rollup":                              KRollup,
	"groupingsets":                        KGroupingSets,
	"lambda":                              KLambda,
	"limit":                               KLimit,
	"limitoptions":                        KLimitOptions,
	"join":                                KJoin,
	"lateral":                             KLateral,
	"tablefromrows":                       KTableFromRows,
	"matchrecognizemeasure":               KMatchRecognizeMeasure,
	"matchrecognize":                      KMatchRecognize,
	"final":                               KFinal,
	"offset":                              KOffset,
	"order":                               KOrder,
	"withfill":                            KWithFill,
	"skipjsoncolumn":                      KSkipJSONColumn,
	"cluster":                             KCluster,
	"distribute":                          KDistribute,
	"sort":                                KSort,
	"qualify":                             KQualify,
	"inputoutputformat":                   KInputOutputFormat,
	"return":                              KReturn,
	"tuple":                               KTuple,
	"queryoption":                         KQueryOption,
	"forclause":                           KForClause,
	"withtablehint":                       KWithTableHint,
	"indextablehint":                      KIndexTableHint,
	"historicaldata":                      KHistoricalData,
	"put":                                 KPut,
	"get":                                 KGet,
	"table":                               KTable,
	"setoperation":                        KSetOperation,
	"union":                               KUnion,
	"except":                              KExcept,
	"intersect":                           KIntersect,
	"values":                              KValues,
	"version":                             KVersion,
	"schema":                              KSchema,
	"lock":                                KLock,
	"select":                              KSelect,
	"subquery":                            KSubquery,
	"tablesample":                         KTableSample,
	"tag":                                 KTag,
	"pivot":                               KPivot,
	"unpivotcolumns":                      KUnpivotColumns,
	"window":                              KWindow,
	"windowspec":                          KWindowSpec,
	"prewhere":                            KPreWhere,
	"where":                               KWhere,
	"analyze":                             KAnalyze,
	"analyzestatistics":                   KAnalyzeStatistics,
	"analyzehistogram":                    KAnalyzeHistogram,
	"analyzesample":                       KAnalyzeSample,
	"analyzelistchainedrows":              KAnalyzeListChainedRows,
	"analyzedelete":                       KAnalyzeDelete,
	"analyzewith":                         KAnalyzeWith,
	"analyzevalidate":                     KAnalyzeValidate,
	"analyzecolumns":                      KAnalyzeColumns,
	"usingdata":                           KUsingData,
	"addpartition":                        KAddPartition,
	"attachoption":                        KAttachOption,
	"droppartition":                       KDropPartition,
	"replacepartition":                    KReplacePartition,
	"translatecharacters":                 KTranslateCharacters,
	"overflowtruncatebehavior":            KOverflowTruncateBehavior,
	"json":                                KJSON,
	"jsonpath":                            KJSONPath,
	"jsonpathpart":                        KJSONPathPart,
	"jsonpathfilter":                      KJSONPathFilter,
	"jsonpathkey":                         KJSONPathKey,
	"jsonpathrecursive":                   KJSONPathRecursive,
	"jsonpathroot":                        KJSONPathRoot,
	"jsonpathscript":                      KJSONPathScript,
	"jsonpathslice":                       KJSONPathSlice,
	"jsonpathselector":                    KJSONPathSelector,
	"jsonpathsubscript":                   KJSONPathSubscript,
	"jsonpathunion":                       KJSONPathUnion,
	"jsonpathwildcard":                    KJSONPathWildcard,
	"formatjson":                          KFormatJson,
	"jsonkeyvalue":                        KJSONKeyValue,
	"jsoncolumndef":                       KJSONColumnDef,
	"jsonschema":                          KJSONSchema,
	"jsonvalue":                           KJSONValue,
	"jsonvaluearray":                      KJSONValueArray,
	"openjsoncolumndef":                   KOpenJSONColumnDef,
	"jsonextractquote":                    KJSONExtractQuote,
	"scoperesolution":                     KScopeResolution,
	"stream":                              KStream,
	"modelattribute":                      KModelAttribute,
	"xmlnamespace":                        KXMLNamespace,
	"xmlkeyvalueoption":                   KXMLKeyValueOption,
	"semicolon":                           KSemicolon,
	"tablecolumn":                         KTableColumn,
	"variadic":                            KVariadic,
	"storedprocedure":                     KStoredProcedure,
	"block":                               KBlock,
	"ifblock":                             KIfBlock,
	"whileblock":                          KWhileBlock,
	"endstatement":                        KEndStatement,
	"ascii":                               KAscii,
	"bitlength":                           KBitLength,
	"bytelength":                          KByteLength,
	"chr":                                 KChr,
	"concat":                              KConcat,
	"concatws":                            KConcatWs,
	"contains":                            KContains,
	"elt":                                 KElt,
	"endswith":                            KEndsWith,
	"format":                              KFormat,
	"initcap":                             KInitcap,
	"isascii":                             KIsAscii,
	"left":                                KLeft,
	"length":                              KLength,
	"levenshtein":                         KLevenshtein,
	"lower":                               KLower,
	"matchagainst":                        KMatchAgainst,
	"normalize":                           KNormalize,
	"numbertostr":                         KNumberToStr,
	"overlay":                             KOverlay,
	"pad":                                 KPad,
	"repeat":                              KRepeat,
	"replace":                             KReplace,
	"reverse":                             KReverse,
	"right":                               KRight,
	"rtrimmedlength":                      KRtrimmedLength,
	"search":                              KSearch,
	"searchip":                            KSearchIp,
	"soundex":                             KSoundex,
	"soundexp123":                         KSoundexP123,
	"space":                               KSpace,
	"split":                               KSplit,
	"splitpart":                           KSplitPart,
	"splittotable":                        KSplitToTable,
	"strtok":                              KStrtok,
	"startswith":                          KStartsWith,
	"strposition":                         KStrPosition,
	"strtomap":                            KStrToMap,
	"string":                              KString,
	"stuff":                               KStuff,
	"substring":                           KSubstring,
	"substringindex":                      KSubstringIndex,
	"translate":                           KTranslate,
	"trim":                                KTrim,
	"unicode":                             KUnicode,
	"upper":                               KUpper,
	"base64decodebinary":                  KBase64DecodeBinary,
	"base64decodestring":                  KBase64DecodeString,
	"base64encode":                        KBase64Encode,
	"codepointstobytes":                   KCodePointsToBytes,
	"codepointstostring":                  KCodePointsToString,
	"converttocharset":                    KConvertToCharset,
	"decode":                              KDecode,
	"encode":                              KEncode,
	"frombase":                            KFromBase,
	"frombase32":                          KFromBase32,
	"frombase64":                          KFromBase64,
	"hex":                                 KHex,
	"hexdecodestring":                     KHexDecodeString,
	"lowerhex":                            KLowerHex,
	"safeconvertbytestostring":            KSafeConvertBytesToString,
	"tobase32":                            KToBase32,
	"tobase64":                            KToBase64,
	"tobinary":                            KToBinary,
	"tochar":                              KToChar,
	"tocodepoints":                        KToCodePoints,
	"todecfloat":                          KToDecfloat,
	"todouble":                            KToDouble,
	"tofile":                              KToFile,
	"tonumber":                            KToNumber,
	"trybase64decodebinary":               KTryBase64DecodeBinary,
	"trybase64decodestring":               KTryBase64DecodeString,
	"tryhexdecodebinary":                  KTryHexDecodeBinary,
	"tryhexdecodestring":                  KTryHexDecodeString,
	"trytodecfloat":                       KTryToDecfloat,
	"unhex":                               KUnhex,
	"regexpcount":                         KRegexpCount,
	"regexpextract":                       KRegexpExtract,
	"regexpextractall":                    KRegexpExtractAll,
	"regexpfullmatch":                     KRegexpFullMatch,
	"regexpilike":                         KRegexpILike,
	"regexpinstr":                         KRegexpInstr,
	"regexpreplace":                       KRegexpReplace,
	"regexpsplit":                         KRegexpSplit,
	"regexpsubstr":                        KRegexpSubstr,
	"compress":                            KCompress,
	"decrypt":                             KDecrypt,
	"decryptraw":                          KDecryptRaw,
	"decompressbinary":                    KDecompressBinary,
	"decompressstring":                    KDecompressString,
	"encrypt":                             KEncrypt,
	"encryptraw":                          KEncryptRaw,
	"cityhash64":                          KCityHash64,
	"farmfingerprint":                     KFarmFingerprint,
	"md5":                                 KMD5,
	"md5digest":                           KMD5Digest,
	"md5numberlower64":                    KMD5NumberLower64,
	"md5numberupper64":                    KMD5NumberUpper64,
	"sha":                                 KSHA,
	"sha1digest":                          KSHA1Digest,
	"sha2":                                KSHA2,
	"sha2digest":                          KSHA2Digest,
	"standardhash":                        KStandardHash,
	"parsebignumeric":                     KParseBignumeric,
	"parsenumeric":                        KParseNumeric,
	"parseurl":                            KParseUrl,
	"currentdate":                         KCurrentDate,
	"currentdatetime":                     KCurrentDatetime,
	"currenttime":                         KCurrentTime,
	"currenttimestamp":                    KCurrentTimestamp,
	"currenttimestampltz":                 KCurrentTimestampLTZ,
	"currenttimezone":                     KCurrentTimezone,
	"localtime":                           KLocaltime,
	"localtimestamp":                      KLocaltimestamp,
	"systimestamp":                        KSystimestamp,
	"utcdate":                             KUtcDate,
	"utctime":                             KUtcTime,
	"utctimestamp":                        KUtcTimestamp,
	"addmonths":                           KAddMonths,
	"dateadd":                             KDateAdd,
	"datebin":                             KDateBin,
	"datediff":                            KDateDiff,
	"datesub":                             KDateSub,
	"datetimeadd":                         KDatetimeAdd,
	"datetimediff":                        KDatetimeDiff,
	"datetimesub":                         KDatetimeSub,
	"monthsbetween":                       KMonthsBetween,
	"timeadd":                             KTimeAdd,
	"timediff":                            KTimeDiff,
	"timesub":                             KTimeSub,
	"timestampadd":                        KTimestampAdd,
	"timestampdiff":                       KTimestampDiff,
	"timestampsub":                        KTimestampSub,
	"tsordsadd":                           KTsOrDsAdd,
	"tsordsdiff":                          KTsOrDsDiff,
	"datetimetrunc":                       KDatetimeTrunc,
	"datetrunc":                           KDateTrunc,
	"timestamptrunc":                      KTimestampTrunc,
	"timeslice":                           KTimeSlice,
	"timetrunc":                           KTimeTrunc,
	"day":                                 KDay,
	"dayofmonth":                          KDayOfMonth,
	"dayofweek":                           KDayOfWeek,
	"dayofweekiso":                        KDayOfWeekIso,
	"dayofyear":                           KDayOfYear,
	"dayname":                             KDayname,
	"extract":                             KExtract,
	"getextract":                          KGetExtract,
	"hour":                                KHour,
	"minute":                              KMinute,
	"month":                               KMonth,
	"monthname":                           KMonthname,
	"quarter":                             KQuarter,
	"second":                              KSecond,
	"todays":                              KToDays,
	"week":                                KWeek,
	"weekofyear":                          KWeekOfYear,
	"year":                                KYear,
	"yearofweek":                          KYearOfWeek,
	"yearofweekiso":                       KYearOfWeekIso,
	"date":                                KDate,
	"datefromparts":                       KDateFromParts,
	"datefromunixdate":                    KDateFromUnixDate,
	"datetime":                            KDatetime,
	"gapfill":                             KGapFill,
	"generatedatearray":                   KGenerateDateArray,
	"generatetimestamparray":              KGenerateTimestampArray,
	"justifydays":                         KJustifyDays,
	"justifyhours":                        KJustifyHours,
	"justifyinterval":                     KJustifyInterval,
	"lastday":                             KLastDay,
	"makeinterval":                        KMakeInterval,
	"nextday":                             KNextDay,
	"previousday":                         KPreviousDay,
	"time":                                KTime,
	"timefromparts":                       KTimeFromParts,
	"timestamp":                           KTimestamp,
	"timestampfromparts":                  KTimestampFromParts,
	"timestampltzfromparts":               KTimestampLtzFromParts,
	"timestamptzfromparts":                KTimestampTzFromParts,
	"converttimezone":                     KConvertTimezone,
	"datestrtodate":                       KDateStrToDate,
	"datetodatestr":                       KDateToDateStr,
	"datetodi":                            KDateToDi,
	"ditodate":                            KDiToDate,
	"fromiso8601timestamp":                KFromISO8601Timestamp,
	"fromiso8601date":                     KFromISO8601Date,
	"fromiso8601timestampnanos":           KFromISO8601TimestampNanos,
	"parsedatetime":                       KParseDatetime,
	"parsetime":                           KParseTime,
	"strtodate":                           KStrToDate,
	"strtotime":                           KStrToTime,
	"strtounix":                           KStrToUnix,
	"timestrtodate":                       KTimeStrToDate,
	"timestrtotime":                       KTimeStrToTime,
	"timestrtounix":                       KTimeStrToUnix,
	"timetostr":                           KTimeToStr,
	"timetotimestr":                       KTimeToTimeStr,
	"timetounix":                          KTimeToUnix,
	"tsorditodi":                          KTsOrDiToDi,
	"tsordstodate":                        KTsOrDsToDate,
	"tsordstodatestr":                     KTsOrDsToDateStr,
	"tsordstodatetime":                    KTsOrDsToDatetime,
	"tsordstotime":                        KTsOrDsToTime,
	"tsordstotimestamp":                   KTsOrDsToTimestamp,
	"unixdate":                            KUnixDate,
	"unixmicros":                          KUnixMicros,
	"unixmillis":                          KUnixMillis,
	"unixseconds":                         KUnixSeconds,
	"unixtostr":                           KUnixToStr,
	"unixtotime":                          KUnixToTime,
	"unixtotimestr":                       KUnixToTimeStr,
	"nearest":                             KNearest,
	"connectto":                           KConnectTo,
	"disconnect":                          KDisconnect,
	"pos":                                 KPos,
	"functionspecification":               KFunctionSpecification,
	"jsonbcontainstopkey":                 KJSONBContainsTopKey,
	"partitionbyexcluding":                KPartitionByExcluding,
	"optimize":                            KOptimize,
	"tablespaceproperty":                  KTableSpaceProperty,
	"per":                                 KPer,
	"physicalproperty":                    KPhysicalProperty,
	"storageproperty":                     KStorageProperty,
	"liststage":                           KListStage,
	"binarycolumnconstraint":              KBinaryColumnConstraint,
	"weightstring":                        KWeightString,
	"copytagsproperty":                    KCopyTagsProperty,
	"replacecolumns":                      KReplaceColumns,
	"withoutrowidproperty":                KWithoutRowidProperty,
	"aggregatekeyproperty":                KAggregateKeyProperty,
	"casestatement":                       KCaseStatement,
	"loopblock":                           KLoopBlock,
	"repeatblock":                         KRepeatBlock,
	"leave":                               KLeave,
	"iterate":                             KIterate,
	"optionsproperty":                     KOptionsProperty,
}

// FUNCTION_BY_NAME maps each SQL function name (uppercase, including aliases) to its function
// kind.
var FUNCTION_BY_NAME = map[string]Kind{
	"ABS":                             KAbs,
	"ACOS":                            KAcos,
	"ACOSH":                           KAcosh,
	"ADD_MONTHS":                      KAddMonths,
	"AI_AGG":                          KAIAgg,
	"AI_CLASSIFY":                     KAIClassify,
	"AI_EMBED":                        KAIEmbed,
	"AI_GENERATE":                     KAIGenerate,
	"AI_SIMILARITY":                   KAISimilarity,
	"AI_SUMMARIZE_AGG":                KAISummarizeAgg,
	"AND":                             KAnd,
	"ANONYMOUS_AGG_FUNC":              KAnonymousAggFunc,
	"ANY_VALUE":                       KAnyValue,
	"APPLY":                           KApply,
	"APPROXIMATE_JACCARD_INDEX":       KApproximateSimilarity,
	"APPROXIMATE_SIMILARITY":          KApproximateSimilarity,
	"APPROX_COUNT_DISTINCT":           KApproxDistinct,
	"APPROX_DISTINCT":                 KApproxDistinct,
	"APPROX_PERCENTILE_ACCUMULATE":    KApproxPercentileAccumulate,
	"APPROX_PERCENTILE_COMBINE":       KApproxPercentileCombine,
	"APPROX_PERCENTILE_ESTIMATE":      KApproxPercentileEstimate,
	"APPROX_QUANTILE":                 KApproxQuantile,
	"APPROX_QUANTILES":                KApproxQuantiles,
	"APPROX_TOP_K":                    KApproxTopK,
	"APPROX_TOP_K_ACCUMULATE":         KApproxTopKAccumulate,
	"APPROX_TOP_K_COMBINE":            KApproxTopKCombine,
	"APPROX_TOP_K_ESTIMATE":           KApproxTopKEstimate,
	"APPROX_TOP_SUM":                  KApproxTopSum,
	"ARGMAX":                          KArgMax,
	"ARGMIN":                          KArgMin,
	"ARG_MAX":                         KArgMax,
	"ARG_MIN":                         KArgMin,
	"ARRAY":                           KArray,
	"ARRAYS_ZIP":                      KArraysZip,
	"ARRAY_AGG":                       KArrayAgg,
	"ARRAY_ALL":                       KArrayAll,
	"ARRAY_ANY":                       KArrayAny,
	"ARRAY_APPEND":                    KArrayAppend,
	"ARRAY_CAT":                       KArrayConcat,
	"ARRAY_COMPACT":                   KArrayCompact,
	"ARRAY_CONCAT":                    KArrayConcat,
	"ARRAY_CONCAT_AGG":                KArrayConcatAgg,
	"ARRAY_CONSTRUCT_COMPACT":         KArrayConstructCompact,
	"ARRAY_CONTAINED_BY":              KArrayContainedBy,
	"ARRAY_CONTAINS":                  KArrayContains,
	"ARRAY_CONTAINS_ALL":              KArrayContainsAll,
	"ARRAY_DISTINCT":                  KArrayDistinct,
	"ARRAY_EXCEPT":                    KArrayExcept,
	"ARRAY_FILTER":                    KArrayFilter,
	"ARRAY_FIRST":                     KArrayFirst,
	"ARRAY_HAS":                       KArrayContains,
	"ARRAY_HAS_ALL":                   KArrayContainsAll,
	"ARRAY_INSERT":                    KArrayInsert,
	"ARRAY_INTERSECT":                 KArrayIntersect,
	"ARRAY_INTERSECTION":              KArrayIntersect,
	"ARRAY_JOIN":                      KArrayToString,
	"ARRAY_LAST":                      KArrayLast,
	"ARRAY_LENGTH":                    KArraySize,
	"ARRAY_MAX":                       KArrayMax,
	"ARRAY_MIN":                       KArrayMin,
	"ARRAY_OVERLAPS":                  KArrayOverlaps,
	"ARRAY_POSITION":                  KArrayPosition,
	"ARRAY_PREPEND":                   KArrayPrepend,
	"ARRAY_REMOVE":                    KArrayRemove,
	"ARRAY_REMOVE_AT":                 KArrayRemoveAt,
	"ARRAY_REVERSE":                   KArrayReverse,
	"ARRAY_SIZE":                      KArraySize,
	"ARRAY_SLICE":                     KArraySlice,
	"ARRAY_SORT":                      KArraySort,
	"ARRAY_SUM":                       KArraySum,
	"ARRAY_TO_STRING":                 KArrayToString,
	"ARRAY_UNION_AGG":                 KArrayUnionAgg,
	"ARRAY_UNIQUE_AGG":                KArrayUniqueAgg,
	"ASCII":                           KAscii,
	"ASIN":                            KAsin,
	"ASINH":                           KAsinh,
	"ATAN":                            KAtan,
	"ATAN2":                           KAtan2,
	"ATANH":                           KAtanh,
	"AVG":                             KAvg,
	"A_I_FORECAST":                    KAIForecast,
	"BASE64_DECODE_BINARY":            KBase64DecodeBinary,
	"BASE64_DECODE_STRING":            KBase64DecodeString,
	"BASE64_ENCODE":                   KBase64Encode,
	"BITMAP_BIT_POSITION":             KBitmapBitPosition,
	"BITMAP_BUCKET_NUMBER":            KBitmapBucketNumber,
	"BITMAP_CONSTRUCT_AGG":            KBitmapConstructAgg,
	"BITMAP_COUNT":                    KBitmapCount,
	"BITMAP_OR_AGG":                   KBitmapOrAgg,
	"BITWISE_AND_AGG":                 KBitwiseAndAgg,
	"BITWISE_COUNT":                   KBitwiseCount,
	"BITWISE_OR_AGG":                  KBitwiseOrAgg,
	"BITWISE_XOR_AGG":                 KBitwiseXorAgg,
	"BIT_LENGTH":                      KBitLength,
	"BOOLAND":                         KBooland,
	"BOOLAND_AGG":                     KLogicalAnd,
	"BOOLNOT":                         KBoolnot,
	"BOOLOR":                          KBoolor,
	"BOOLOR_AGG":                      KLogicalOr,
	"BOOLXOR_AGG":                     KBoolxorAgg,
	"BOOL_AND":                        KLogicalAnd,
	"BOOL_OR":                         KLogicalOr,
	"BYTE_LENGTH":                     KByteLength,
	"CASE":                            KCase,
	"CAST":                            KCast,
	"CAST_TO_STR_TYPE":                KCastToStrType,
	"CBRT":                            KCbrt,
	"CEIL":                            KCeil,
	"CEILING":                         KCeil,
	"CHAR":                            KChr,
	"CHARACTER_LENGTH":                KLength,
	"CHAR_LENGTH":                     KLength,
	"CHECK_JSON":                      KCheckJson,
	"CHECK_XML":                       KCheckXml,
	"CHR":                             KChr,
	"CITY_HASH64":                     KCityHash64,
	"COALESCE":                        KCoalesce,
	"CODE_POINTS_TO_BYTES":            KCodePointsToBytes,
	"CODE_POINTS_TO_STRING":           KCodePointsToString,
	"COLLATE":                         KCollate,
	"COLLATION":                       KCollation,
	"COLUMNS":                         KColumns,
	"COMBINED_AGG_FUNC":               KCombinedAggFunc,
	"COMBINED_PARAMETERIZED_AGG":      KCombinedParameterizedAgg,
	"COMPRESS":                        KCompress,
	"CONCAT":                          KConcat,
	"CONCAT_WS":                       KConcatWs,
	"CONNECT_BY_ROOT":                 KConnectByRoot,
	"CONTAINS":                        KContains,
	"CONVERT":                         KConvert,
	"CONVERT_TIMEZONE":                KConvertTimezone,
	"CONVERT_TO_CHARSET":              KConvertToCharset,
	"CORR":                            KCorr,
	"COS":                             KCos,
	"COSH":                            KCosh,
	"COSINE_DISTANCE":                 KCosineDistance,
	"COT":                             KCot,
	"COTH":                            KCoth,
	"COUNT":                           KCount,
	"COUNTIF":                         KCountIf,
	"COUNT_IF":                        KCountIf,
	"COVAR_POP":                       KCovarPop,
	"COVAR_SAMP":                      KCovarSamp,
	"CSC":                             KCsc,
	"CSCH":                            KCsch,
	"CUME_DIST":                       KCumeDist,
	"CURRENT_ACCOUNT":                 KCurrentAccount,
	"CURRENT_ACCOUNT_NAME":            KCurrentAccountName,
	"CURRENT_AVAILABLE_ROLES":         KCurrentAvailableRoles,
	"CURRENT_CATALOG":                 KCurrentCatalog,
	"CURRENT_CLIENT":                  KCurrentClient,
	"CURRENT_DATABASE":                KCurrentDatabase,
	"CURRENT_DATE":                    KCurrentDate,
	"CURRENT_DATETIME":                KCurrentDatetime,
	"CURRENT_IP_ADDRESS":              KCurrentIpAddress,
	"CURRENT_ORGANIZATION_NAME":       KCurrentOrganizationName,
	"CURRENT_ORGANIZATION_USER":       KCurrentOrganizationUser,
	"CURRENT_REGION":                  KCurrentRegion,
	"CURRENT_ROLE":                    KCurrentRole,
	"CURRENT_ROLE_TYPE":               KCurrentRoleType,
	"CURRENT_SCHEMA":                  KCurrentSchema,
	"CURRENT_SCHEMAS":                 KCurrentSchemas,
	"CURRENT_SECONDARY_ROLES":         KCurrentSecondaryRoles,
	"CURRENT_SESSION":                 KCurrentSession,
	"CURRENT_STATEMENT":               KCurrentStatement,
	"CURRENT_TIME":                    KCurrentTime,
	"CURRENT_TIMESTAMP":               KCurrentTimestamp,
	"CURRENT_TIMESTAMP_L_T_Z":         KCurrentTimestampLTZ,
	"CURRENT_TIMEZONE":                KCurrentTimezone,
	"CURRENT_TRANSACTION":             KCurrentTransaction,
	"CURRENT_USER":                    KCurrentUser,
	"CURRENT_USER_ID":                 KCurrentUserId,
	"CURRENT_VERSION":                 KCurrentVersion,
	"CURRENT_WAREHOUSE":               KCurrentWarehouse,
	"DATE":                            KDate,
	"DATEDIFF":                        KDateDiff,
	"DATEFROMPARTS":                   KDateFromParts,
	"DATETIME":                        KDatetime,
	"DATETIME_ADD":                    KDatetimeAdd,
	"DATETIME_DIFF":                   KDatetimeDiff,
	"DATETIME_SUB":                    KDatetimeSub,
	"DATETIME_TRUNC":                  KDatetimeTrunc,
	"DATE_ADD":                        KDateAdd,
	"DATE_BIN":                        KDateBin,
	"DATE_DIFF":                       KDateDiff,
	"DATE_FROM_PARTS":                 KDateFromParts,
	"DATE_FROM_UNIX_DATE":             KDateFromUnixDate,
	"DATE_STR_TO_DATE":                KDateStrToDate,
	"DATE_SUB":                        KDateSub,
	"DATE_TO_DATE_STR":                KDateToDateStr,
	"DATE_TO_DI":                      KDateToDi,
	"DATE_TRUNC":                      KDateTrunc,
	"DAY":                             KDay,
	"DAYNAME":                         KDayname,
	"DAYOFMONTH":                      KDayOfMonth,
	"DAYOFWEEK":                       KDayOfWeek,
	"DAYOFWEEK_ISO":                   KDayOfWeekIso,
	"DAYOFYEAR":                       KDayOfYear,
	"DAY_OF_MONTH":                    KDayOfMonth,
	"DAY_OF_WEEK":                     KDayOfWeek,
	"DAY_OF_YEAR":                     KDayOfYear,
	"DECODE":                          KDecode,
	"DECODE_CASE":                     KDecodeCase,
	"DECOMPRESS_BINARY":               KDecompressBinary,
	"DECOMPRESS_STRING":               KDecompressString,
	"DECRYPT":                         KDecrypt,
	"DECRYPT_RAW":                     KDecryptRaw,
	"DEGREES":                         KDegrees,
	"DENSE_RANK":                      KDenseRank,
	"DI_TO_DATE":                      KDiToDate,
	"DOT_PRODUCT":                     KDotProduct,
	"DYNAMIC_IDENTIFIER":              KDynamicIdentifier,
	"ELT":                             KElt,
	"ENCODE":                          KEncode,
	"ENCRYPT":                         KEncrypt,
	"ENCRYPT_RAW":                     KEncryptRaw,
	"ENDSWITH":                        KEndsWith,
	"ENDS_WITH":                       KEndsWith,
	"EQUAL_NULL":                      KEqualNull,
	"EUCLIDEAN_DISTANCE":              KEuclideanDistance,
	"EXISTS":                          KExists,
	"EXP":                             KExp,
	"EXPLODE":                         KExplode,
	"EXPLODE_OUTER":                   K_ExplodeOuter,
	"EXPLODING_GENERATE_SERIES":       KExplodingGenerateSeries,
	"EXTRACT":                         KExtract,
	"FACTORIAL":                       KFactorial,
	"FARMFINGERPRINT64":               KFarmFingerprint,
	"FARM_FINGERPRINT":                KFarmFingerprint,
	"FEATURES_AT_TIME":                KFeaturesAtTime,
	"FILTER":                          KArrayFilter,
	"FIRST":                           KFirst,
	"FIRST_VALUE":                     KFirstValue,
	"FLATTEN":                         KFlatten,
	"FLOAT64":                         KFloat64,
	"FLOOR":                           KFloor,
	"FORMAT":                          KFormat,
	"FROM_BASE":                       KFromBase,
	"FROM_BASE32":                     KFromBase32,
	"FROM_BASE64":                     KFromBase64,
	"FROM_ISO8601_DATE":               KFromISO8601Date,
	"FROM_ISO8601_TIMESTAMP":          KFromISO8601Timestamp,
	"FROM_ISO8601_TIMESTAMP_NANOS":    KFromISO8601TimestampNanos,
	"GAP_FILL":                        KGapFill,
	"GENERATE_BOOL":                   KGenerateBool,
	"GENERATE_DATE_ARRAY":             KGenerateDateArray,
	"GENERATE_DOUBLE":                 KGenerateDouble,
	"GENERATE_EMBEDDING":              KGenerateEmbedding,
	"GENERATE_INT":                    KGenerateInt,
	"GENERATE_SERIES":                 KGenerateSeries,
	"GENERATE_TABLE":                  KGenerateTable,
	"GENERATE_TEXT":                   KGenerateText,
	"GENERATE_TIMESTAMP_ARRAY":        KGenerateTimestampArray,
	"GENERATE_UUID":                   KUuid,
	"GENERATOR":                       KGenerator,
	"GEN_RANDOM_UUID":                 KUuid,
	"GETBIT":                          KGetbit,
	"GET_BIT":                         KGetbit,
	"GET_EXTRACT":                     KGetExtract,
	"GET_IGNORE_CASE":                 KGetIgnoreCase,
	"GREATEST":                        KGreatest,
	"GROUPING":                        KGrouping,
	"GROUPING_ID":                     KGroupingId,
	"GROUP_CONCAT":                    KGroupConcat,
	"HASH_AGG":                        KHashAgg,
	"HEX":                             KHex,
	"HEX_DECODE_STRING":               KHexDecodeString,
	"HLL":                             KHll,
	"HOST":                            KHost,
	"HOUR":                            KHour,
	"IF":                              KIf,
	"IFNULL":                          KCoalesce,
	"IIF":                             KIf,
	"INITCAP":                         KInitcap,
	"INLINE":                          KInline,
	"INSERT":                          KStuff,
	"INT64":                           KInt64,
	"ISINF":                           KIsInf,
	"ISNAN":                           KIsNan,
	"ISODOW":                          KDayOfWeekIso,
	"IS_ARRAY":                        KIsArray,
	"IS_ASCII":                        KIsAscii,
	"IS_INF":                          KIsInf,
	"IS_NAN":                          KIsNan,
	"IS_NULL_VALUE":                   KIsNullValue,
	"JAROWINKLER_SIMILARITY":          KJarowinklerSimilarity,
	"JSONB_CONTAINS":                  KJSONBContains,
	"JSONB_EXISTS":                    KJSONBExists,
	"JSONB_EXTRACT":                   KJSONBExtract,
	"JSONB_EXTRACT_SCALAR":            KJSONBExtractScalar,
	"JSON_ARRAY_APPEND":               KJSONArrayAppend,
	"JSON_ARRAY_CONTAINS":             KJSONArrayContains,
	"JSON_ARRAY_INSERT":               KJSONArrayInsert,
	"JSON_EXTRACT":                    KJSONExtract,
	"JSON_EXTRACT_ARRAY":              KJSONExtractArray,
	"JSON_EXTRACT_SCALAR":             KJSONExtractScalar,
	"JSON_FORMAT":                     KJSONFormat,
	"JSON_KEYS":                       KJSONKeys,
	"JSON_PARSE":                      KParseJSON,
	"JSON_REMOVE":                     KJSONRemove,
	"JSON_SET":                        KJSONSet,
	"JSON_STRIP_NULLS":                KJSONStripNulls,
	"JSON_TYPE":                       KJSONType,
	"JUSTIFY_DAYS":                    KJustifyDays,
	"JUSTIFY_HOURS":                   KJustifyHours,
	"JUSTIFY_INTERVAL":                KJustifyInterval,
	"J_S_O_N_ARRAY":                   KJSONArray,
	"J_S_O_N_ARRAY_AGG":               KJSONArrayAgg,
	"J_S_O_N_BOOL":                    KJSONBool,
	"J_S_O_N_B_CONTAINS_ALL_TOP_KEYS": KJSONBContainsAllTopKeys,
	"J_S_O_N_B_CONTAINS_TOP_KEY":      KJSONBContainsTopKey,
	"J_S_O_N_B_CONTAINS_ANY_TOP_KEYS": KJSONBContainsAnyTopKeys,
	"J_S_O_N_B_DELETE_AT_PATH":        KJSONBDeleteAtPath,
	"J_S_O_N_B_OBJECT_AGG":            KJSONBObjectAgg,
	"J_S_O_N_B_PATH_EXISTS":           KJSONBPathExists,
	"J_S_O_N_CAST":                    KJSONCast,
	"J_S_O_N_EXISTS":                  KJSONExists,
	"J_S_O_N_KEYS_AT_DEPTH":           KJSONKeysAtDepth,
	"J_S_O_N_OBJECT":                  KJSONObject,
	"J_S_O_N_OBJECT_AGG":              KJSONObjectAgg,
	"J_S_O_N_TABLE":                   KJSONTable,
	"J_S_O_N_VALUE_ARRAY":             KJSONValueArray,
	"KURTOSIS":                        KKurtosis,
	"LAG":                             KLag,
	"LAST":                            KLast,
	"LAST_DAY":                        KLastDay,
	"LAST_DAY_OF_MONTH":               KLastDay,
	"LAST_VALUE":                      KLastValue,
	"LAX_BOOL":                        KLaxBool,
	"LAX_FLOAT64":                     KLaxFloat64,
	"LAX_INT64":                       KLaxInt64,
	"LAX_STRING":                      KLaxString,
	"LCASE":                           KLower,
	"LEAD":                            KLead,
	"LEAST":                           KLeast,
	"LEFT":                            KLeft,
	"LEN":                             KLength,
	"LENGTH":                          KLength,
	"LEVENSHTEIN":                     KLevenshtein,
	"LIST":                            KList,
	"LN":                              KLn,
	"LOCALTIME":                       KLocaltime,
	"LOCALTIMESTAMP":                  KLocaltimestamp,
	"LOG":                             KLog,
	"LOGICAL_AND":                     KLogicalAnd,
	"LOGICAL_OR":                      KLogicalOr,
	"LOWER":                           KLower,
	"LOWER_HEX":                       KLowerHex,
	"MAKE_INTERVAL":                   KMakeInterval,
	"MANHATTAN_DISTANCE":              KManhattanDistance,
	"MAP":                             KMap,
	"MAP_CAT":                         KMapCat,
	"MAP_CONTAINS_KEY":                KMapContainsKey,
	"MAP_DELETE":                      KMapDelete,
	"MAP_FROM_ENTRIES":                KMapFromEntries,
	"MAP_INSERT":                      KMapInsert,
	"MAP_KEYS":                        KMapKeys,
	"MAP_PICK":                        KMapPick,
	"MAP_SIZE":                        KMapSize,
	"MATCH_AGAINST":                   KMatchAgainst,
	"MAX":                             KMax,
	"MAX_BY":                          KArgMax,
	"MD5":                             KMD5,
	"MD5_DIGEST":                      KMD5Digest,
	"MEDIAN":                          KMedian,
	"MIN":                             KMin,
	"MINHASH":                         KMinhash,
	"MINHASH_COMBINE":                 KMinhashCombine,
	"MINUTE":                          KMinute,
	"MIN_BY":                          KArgMin,
	"MODE":                            KMode,
	"MONTH":                           KMonth,
	"MONTHNAME":                       KMonthname,
	"MONTHS_BETWEEN":                  KMonthsBetween,
	"M_D5_NUMBER_LOWER64":             KMD5NumberLower64,
	"M_D5_NUMBER_UPPER64":             KMD5NumberUpper64,
	"M_L_FORECAST":                    KMLForecast,
	"M_L_TRANSLATE":                   KMLTranslate,
	"NET_FUNC":                        KNetFunc,
	"NEXT_DAY":                        KNextDay,
	"NEXT_VALUE_FOR":                  KNextValueFor,
	"NORMAL":                          KNormal,
	"NORMALIZE":                       KNormalize,
	"NTH_VALUE":                       KNthValue,
	"NTILE":                           KNtile,
	"NULLIF":                          KNullif,
	"NUMBER_TO_STR":                   KNumberToStr,
	"NVL":                             KCoalesce,
	"NVL2":                            KNvl2,
	"OBJECT_AGG":                      KObjectAgg,
	"OBJECT_ID":                       KObjectId,
	"OBJECT_INSERT":                   KObjectInsert,
	"OBJECT_TRANSFORM":                KObjectTransform,
	"OPEN_J_S_O_N":                    KOpenJSON,
	"OR":                              KOr,
	"OVERLAY":                         KOverlay,
	"PAD":                             KPad,
	"PARAMETERIZED_AGG":               KParameterizedAgg,
	"PARSE_BIGNUMERIC":                KParseBignumeric,
	"PARSE_DATETIME":                  KParseDatetime,
	"PARSE_IP":                        KParseIp,
	"PARSE_JSON":                      KParseJSON,
	"PARSE_NUMERIC":                   KParseNumeric,
	"PARSE_TIME":                      KParseTime,
	"PARSE_URL":                       KParseUrl,
	"PERCENTILE_CONT":                 KPercentileCont,
	"PERCENTILE_DISC":                 KPercentileDisc,
	"PERCENT_RANK":                    KPercentRank,
	"PI":                              KPi,
	"POSEXPLODE":                      KPosexplode,
	"POSEXPLODE_OUTER":                KPosexplodeOuter,
	"POW":                             KPow,
	"POWER":                           KPow,
	"PREDICT":                         KPredict,
	"PREVIOUS_DAY":                    KPreviousDay,
	"QUANTILE":                        KQuantile,
	"QUARTER":                         KQuarter,
	"RADIANS":                         KRadians,
	"RAND":                            KRand,
	"RANDN":                           KRandn,
	"RANDOM":                          KRand,
	"RANDSTR":                         KRandstr,
	"RANGE_BUCKET":                    KRangeBucket,
	"RANGE_N":                         KRangeN,
	"RANK":                            KRank,
	"READ_CSV":                        KReadCSV,
	"READ_PARQUET":                    KReadParquet,
	"REDUCE":                          KReduce,
	"REGEXP_COUNT":                    KRegexpCount,
	"REGEXP_EXTRACT":                  KRegexpExtract,
	"REGEXP_EXTRACT_ALL":              KRegexpExtractAll,
	"REGEXP_FULL_MATCH":               KRegexpFullMatch,
	"REGEXP_INSTR":                    KRegexpInstr,
	"REGEXP_I_LIKE":                   KRegexpILike,
	"REGEXP_LIKE":                     KRegexpLike,
	"REGEXP_REPLACE":                  KRegexpReplace,
	"REGEXP_SPLIT":                    KRegexpSplit,
	"REGEXP_SUBSTR":                   KRegexpSubstr,
	"REGR_AVGX":                       KRegrAvgx,
	"REGR_AVGY":                       KRegrAvgy,
	"REGR_COUNT":                      KRegrCount,
	"REGR_INTERCEPT":                  KRegrIntercept,
	"REGR_R2":                         KRegrR2,
	"REGR_SLOPE":                      KRegrSlope,
	"REGR_SXX":                        KRegrSxx,
	"REGR_SXY":                        KRegrSxy,
	"REGR_SYY":                        KRegrSyy,
	"REGR_VALX":                       KRegrValx,
	"REGR_VALY":                       KRegrValy,
	"REG_DOMAIN":                      KRegDomain,
	"REPEAT":                          KRepeat,
	"REPLACE":                         KReplace,
	"REVERSE":                         KReverse,
	"RIGHT":                           KRight,
	"RINT":                            KRint,
	"ROUND":                           KRound,
	"ROW_NUMBER":                      KRowNumber,
	"RTRIMMED_LENGTH":                 KRtrimmedLength,
	"SAFE_ADD":                        KSafeAdd,
	"SAFE_CONVERT_BYTES_TO_STRING":    KSafeConvertBytesToString,
	"SAFE_DIVIDE":                     KSafeDivide,
	"SAFE_FUNC":                       KSafeFunc,
	"SAFE_MULTIPLY":                   KSafeMultiply,
	"SAFE_NEGATE":                     KSafeNegate,
	"SAFE_SUBTRACT":                   KSafeSubtract,
	"SEARCH":                          KSearch,
	"SEARCH_IP":                       KSearchIp,
	"SEC":                             KSec,
	"SECH":                            KSech,
	"SECOND":                          KSecond,
	"SEQ1":                            KSeq1,
	"SEQ2":                            KSeq2,
	"SEQ4":                            KSeq4,
	"SEQ8":                            KSeq8,
	"SESSION_USER":                    KSessionUser,
	"SHA":                             KSHA,
	"SHA1":                            KSHA,
	"SHA2":                            KSHA2,
	"SIGN":                            KSign,
	"SIGNUM":                          KSign,
	"SIN":                             KSin,
	"SINH":                            KSinh,
	"SKEWNESS":                        KSkewness,
	"SORT_ARRAY":                      KSortArray,
	"SOUNDEX":                         KSoundex,
	"SOUNDEX_P123":                    KSoundexP123,
	"SPACE":                           KSpace,
	"SPLIT":                           KSplit,
	"SPLIT_BY_STRING":                 KStringToArray,
	"SPLIT_PART":                      KSplitPart,
	"SPLIT_TO_TABLE":                  KSplitToTable,
	"SQRT":                            KSqrt,
	"STANDARD_HASH":                   KStandardHash,
	"STARTSWITH":                      KStartsWith,
	"STARTS_WITH":                     KStartsWith,
	"STAR_MAP":                        KStarMap,
	"STDDEV":                          KStddev,
	"STDDEV_POP":                      KStddevPop,
	"STDDEV_SAMP":                     KStddevSamp,
	"STDEV":                           KStddev,
	"STRING":                          KString,
	"STRING_TO_ARRAY":                 KStringToArray,
	"STRIP_NULL_VALUE":                KStripNullValue,
	"STRTOK":                          KStrtok,
	"STRTOK_TO_ARRAY":                 KStrtokToArray,
	"STRUCT":                          KStruct,
	"STRUCT_EXTRACT":                  KStructExtract,
	"STR_POSITION":                    KStrPosition,
	"STR_TO_DATE":                     KStrToDate,
	"STR_TO_MAP":                      KStrToMap,
	"STR_TO_TIME":                     KStrToTime,
	"STR_TO_UNIX":                     KStrToUnix,
	"STUFF":                           KStuff,
	"ST_DISTANCE":                     KStDistance,
	"ST_MAKEPOINT":                    KStPoint,
	"ST_POINT":                        KStPoint,
	"SUBSTR":                          KSubstring,
	"SUBSTRING":                       KSubstring,
	"SUBSTRING_INDEX":                 KSubstringIndex,
	"SUM":                             KSum,
	"SYSTIMESTAMP":                    KSystimestamp,
	"S_H_A1_DIGEST":                   KSHA1Digest,
	"S_H_A2_DIGEST":                   KSHA2Digest,
	"TAN":                             KTan,
	"TANH":                            KTanh,
	"TIME":                            KTime,
	"TIMEFROMPARTS":                   KTimeFromParts,
	"TIMESTAMP":                       KTimestamp,
	"TIMESTAMPDIFF":                   KTimestampDiff,
	"TIMESTAMPFROMPARTS":              KTimestampFromParts,
	"TIMESTAMPLTZFROMPARTS":           KTimestampLtzFromParts,
	"TIMESTAMPTZFROMPARTS":            KTimestampTzFromParts,
	"TIMESTAMP_ADD":                   KTimestampAdd,
	"TIMESTAMP_DIFF":                  KTimestampDiff,
	"TIMESTAMP_FROM_PARTS":            KTimestampFromParts,
	"TIMESTAMP_LTZ_FROM_PARTS":        KTimestampLtzFromParts,
	"TIMESTAMP_SUB":                   KTimestampSub,
	"TIMESTAMP_TRUNC":                 KTimestampTrunc,
	"TIMESTAMP_TZ_FROM_PARTS":         KTimestampTzFromParts,
	"TIME_ADD":                        KTimeAdd,
	"TIME_DIFF":                       KTimeDiff,
	"TIME_FROM_PARTS":                 KTimeFromParts,
	"TIME_SLICE":                      KTimeSlice,
	"TIME_STR_TO_DATE":                KTimeStrToDate,
	"TIME_STR_TO_TIME":                KTimeStrToTime,
	"TIME_STR_TO_UNIX":                KTimeStrToUnix,
	"TIME_SUB":                        KTimeSub,
	"TIME_TO_STR":                     KTimeToStr,
	"TIME_TO_TIME_STR":                KTimeToTimeStr,
	"TIME_TO_UNIX":                    KTimeToUnix,
	"TIME_TRUNC":                      KTimeTrunc,
	"TO_ARRAY":                        KToArray,
	"TO_BASE32":                       KToBase32,
	"TO_BASE64":                       KToBase64,
	"TO_BINARY":                       KToBinary,
	"TO_BOOLEAN":                      KToBoolean,
	"TO_CHAR":                         KToChar,
	"TO_CODE_POINTS":                  KToCodePoints,
	"TO_DAYS":                         KToDays,
	"TO_DECFLOAT":                     KToDecfloat,
	"TO_DOUBLE":                       KToDouble,
	"TO_FILE":                         KToFile,
	"TO_MAP":                          KToMap,
	"TO_NUMBER":                       KToNumber,
	"TO_VARIANT":                      KToVariant,
	"TRANSFORM":                       KTransform,
	"TRANSLATE":                       KTranslate,
	"TRIM":                            KTrim,
	"TRUNC":                           KTrunc,
	"TRUNCATE":                        KTrunc,
	"TRY":                             KTry,
	"TRY_BASE64_DECODE_BINARY":        KTryBase64DecodeBinary,
	"TRY_BASE64_DECODE_STRING":        KTryBase64DecodeString,
	"TRY_CAST":                        KTryCast,
	"TRY_HEX_DECODE_BINARY":           KTryHexDecodeBinary,
	"TRY_HEX_DECODE_STRING":           KTryHexDecodeString,
	"TRY_TO_DECFLOAT":                 KTryToDecfloat,
	"TS_OR_DI_TO_DI":                  KTsOrDiToDi,
	"TS_OR_DS_ADD":                    KTsOrDsAdd,
	"TS_OR_DS_DIFF":                   KTsOrDsDiff,
	"TS_OR_DS_TO_DATE":                KTsOrDsToDate,
	"TS_OR_DS_TO_DATETIME":            KTsOrDsToDatetime,
	"TS_OR_DS_TO_DATE_STR":            KTsOrDsToDateStr,
	"TS_OR_DS_TO_TIME":                KTsOrDsToTime,
	"TS_OR_DS_TO_TIMESTAMP":           KTsOrDsToTimestamp,
	"TYPEOF":                          KTypeof,
	"UCASE":                           KUpper,
	"UNHEX":                           KUnhex,
	"UNICODE":                         KUnicode,
	"UNIFORM":                         KUniform,
	"UNIX_DATE":                       KUnixDate,
	"UNIX_MICROS":                     KUnixMicros,
	"UNIX_MILLIS":                     KUnixMillis,
	"UNIX_SECONDS":                    KUnixSeconds,
	"UNIX_TO_STR":                     KUnixToStr,
	"UNIX_TO_TIME":                    KUnixToTime,
	"UNIX_TO_TIME_STR":                KUnixToTimeStr,
	"UNNEST":                          KUnnest,
	"UPPER":                           KUpper,
	"UTC_DATE":                        KUtcDate,
	"UTC_TIME":                        KUtcTime,
	"UTC_TIMESTAMP":                   KUtcTimestamp,
	"UUID":                            KUuid,
	"UUID_STRING":                     KUuid,
	"VARIANCE":                        KVariance,
	"VARIANCE_POP":                    KVariancePop,
	"VARIANCE_SAMP":                   KVariance,
	"VAR_MAP":                         KVarMap,
	"VAR_POP":                         KVariancePop,
	"VAR_SAMP":                        KVariance,
	"VECTOR_SEARCH":                   KVectorSearch,
	"WEEK":                            KWeek,
	"WEEKOFYEAR":                      KWeekOfYear,
	"WEIGHT_STRING":                   KWeightString,
	"WEEK_OF_YEAR":                    KWeekOfYear,
	"WEEK_START":                      KWeekStart,
	"WIDTH_BUCKET":                    KWidthBucket,
	"XMLELEMENT":                      KXMLElement,
	"XMLGET":                          KXMLGet,
	"XOR":                             KXor,
	"X_M_L_TABLE":                     KXMLTable,
	"YEAR":                            KYear,
	"YEAROFWEEK":                      KYearOfWeek,
	"YEAROFWEEKISO":                   KYearOfWeekIso,
	"YEAR_OF_WEEK":                    KYearOfWeek,
	"YEAR_OF_WEEK_ISO":                KYearOfWeekIso,
	"ZIPF":                            KZipf,
}

// ALL_FUNCTIONS lists every function kind, sorted by kind name.
var ALL_FUNCTIONS = []Kind{KAIAgg, KAIClassify, KAIEmbed, KAIForecast, KAIGenerate, KAISimilarity, KAISummarizeAgg, KAbs, KAcos, KAcosh, KAddMonths, KAnd, KAnonymousAggFunc, KAnyValue, KApply, KApproxDistinct, KApproxPercentileAccumulate, KApproxPercentileCombine, KApproxPercentileEstimate, KApproxQuantile, KApproxQuantiles, KApproxTopK, KApproxTopKAccumulate, KApproxTopKCombine, KApproxTopKEstimate, KApproxTopSum, KApproximateSimilarity, KArgMax, KArgMin, KArray, KArrayAgg, KArrayAll, KArrayAny, KArrayAppend, KArrayCompact, KArrayConcat, KArrayConcatAgg, KArrayConstructCompact, KArrayContainedBy, KArrayContains, KArrayContainsAll, KArrayDistinct, KArrayExcept, KArrayFilter, KArrayFirst, KArrayInsert, KArrayIntersect, KArrayLast, KArrayMax, KArrayMin, KArrayOverlaps, KArrayPosition, KArrayPrepend, KArrayRemove, KArrayRemoveAt, KArrayReverse, KArraySize, KArraySlice, KArraySort, KArraySum, KArrayToString, KArrayUnionAgg, KArrayUniqueAgg, KArraysZip, KAscii, KAsin, KAsinh, KAtan, KAtan2, KAtanh, KAvg, KBase64DecodeBinary, KBase64DecodeString, KBase64Encode, KBitLength, KBitmapBitPosition, KBitmapBucketNumber, KBitmapConstructAgg, KBitmapCount, KBitmapOrAgg, KBitwiseAndAgg, KBitwiseCount, KBitwiseOrAgg, KBitwiseXorAgg, KBooland, KBoolnot, KBoolor, KBoolxorAgg, KByteLength, KCase, KCast, KCastToStrType, KCbrt, KCeil, KCheckJson, KCheckXml, KChr, KCityHash64, KCoalesce, KCodePointsToBytes, KCodePointsToString, KCollate, KCollation, KColumns, KCombinedAggFunc, KCombinedParameterizedAgg, KCompress, KConcat, KConcatWs, KConnectByRoot, KContains, KConvert, KConvertTimezone, KConvertToCharset, KCorr, KCos, KCosh, KCosineDistance, KCot, KCoth, KCount, KCountIf, KCovarPop, KCovarSamp, KCsc, KCsch, KCumeDist, KCurrentAccount, KCurrentAccountName, KCurrentAvailableRoles, KCurrentCatalog, KCurrentClient, KCurrentDatabase, KCurrentDate, KCurrentDatetime, KCurrentIpAddress, KCurrentOrganizationName, KCurrentOrganizationUser, KCurrentRegion, KCurrentRole, KCurrentRoleType, KCurrentSchema, KCurrentSchemas, KCurrentSecondaryRoles, KCurrentSession, KCurrentStatement, KCurrentTime, KCurrentTimestamp, KCurrentTimestampLTZ, KCurrentTimezone, KCurrentTransaction, KCurrentUser, KCurrentUserId, KCurrentVersion, KCurrentWarehouse, KDate, KDateAdd, KDateBin, KDateDiff, KDateFromParts, KDateFromUnixDate, KDateStrToDate, KDateSub, KDateToDateStr, KDateToDi, KDateTrunc, KDatetime, KDatetimeAdd, KDatetimeDiff, KDatetimeSub, KDatetimeTrunc, KDay, KDayOfMonth, KDayOfWeek, KDayOfWeekIso, KDayOfYear, KDayname, KDecode, KDecodeCase, KDecompressBinary, KDecompressString, KDecrypt, KDecryptRaw, KDegrees, KDenseRank, KDiToDate, KDotProduct, KDynamicIdentifier, KElt, KEncode, KEncrypt, KEncryptRaw, KEndsWith, KEqualNull, KEuclideanDistance, KExists, KExp, KExplode, KExplodingGenerateSeries, KExtract, KFactorial, KFarmFingerprint, KFeaturesAtTime, KFirst, KFirstValue, KFlatten, KFloat64, KFloor, KFormat, KFromBase, KFromBase32, KFromBase64, KFromISO8601Date, KFromISO8601Timestamp, KFromISO8601TimestampNanos, KGapFill, KGenerateBool, KGenerateDateArray, KGenerateDouble, KGenerateEmbedding, KGenerateInt, KGenerateSeries, KGenerateTable, KGenerateText, KGenerateTimestampArray, KGenerator, KGetExtract, KGetIgnoreCase, KGetbit, KGreatest, KGroupConcat, KGrouping, KGroupingId, KHashAgg, KHex, KHexDecodeString, KHll, KHost, KHour, KIf, KInitcap, KInline, KInt64, KIsArray, KIsAscii, KIsInf, KIsNan, KIsNullValue, KJSONArray, KJSONArrayAgg, KJSONArrayAppend, KJSONArrayContains, KJSONArrayInsert, KJSONBContains, KJSONBContainsAllTopKeys, KJSONBContainsAnyTopKeys, KJSONBContainsTopKey, KJSONBDeleteAtPath, KJSONBExists, KJSONBExtract, KJSONBExtractScalar, KJSONBObjectAgg, KJSONBPathExists, KJSONBool, KJSONCast, KJSONExists, KJSONExtract, KJSONExtractArray, KJSONExtractScalar, KJSONFormat, KJSONKeys, KJSONKeysAtDepth, KJSONObject, KJSONObjectAgg, KJSONRemove, KJSONSet, KJSONStripNulls, KJSONTable, KJSONType, KJSONValueArray, KJarowinklerSimilarity, KJustifyDays, KJustifyHours, KJustifyInterval, KKurtosis, KLag, KLast, KLastDay, KLastValue, KLaxBool, KLaxFloat64, KLaxInt64, KLaxString, KLead, KLeast, KLeft, KLength, KLevenshtein, KList, KLn, KLocaltime, KLocaltimestamp, KLog, KLogicalAnd, KLogicalOr, KLower, KLowerHex, KMD5, KMD5Digest, KMD5NumberLower64, KMD5NumberUpper64, KMLForecast, KMLTranslate, KMakeInterval, KManhattanDistance, KMap, KMapCat, KMapContainsKey, KMapDelete, KMapFromEntries, KMapInsert, KMapKeys, KMapPick, KMapSize, KMatchAgainst, KMax, KMedian, KMin, KMinhash, KMinhashCombine, KMinute, KMode, KMonth, KMonthname, KMonthsBetween, KNetFunc, KNextDay, KNextValueFor, KNormal, KNormalize, KNthValue, KNtile, KNullif, KNumberToStr, KNvl2, KObjectAgg, KObjectId, KObjectInsert, KObjectTransform, KOpenJSON, KOr, KOverlay, KPad, KParameterizedAgg, KParseBignumeric, KParseDatetime, KParseIp, KParseJSON, KParseNumeric, KParseTime, KParseUrl, KPercentRank, KPercentileCont, KPercentileDisc, KPi, KPosexplode, KPosexplodeOuter, KPow, KPredict, KPreviousDay, KQuantile, KQuarter, KRadians, KRand, KRandn, KRandstr, KRangeBucket, KRangeN, KRank, KReadCSV, KReadParquet, KReduce, KRegDomain, KRegexpCount, KRegexpExtract, KRegexpExtractAll, KRegexpFullMatch, KRegexpILike, KRegexpInstr, KRegexpLike, KRegexpReplace, KRegexpSplit, KRegexpSubstr, KRegrAvgx, KRegrAvgy, KRegrCount, KRegrIntercept, KRegrR2, KRegrSlope, KRegrSxx, KRegrSxy, KRegrSyy, KRegrValx, KRegrValy, KRepeat, KReplace, KReverse, KRight, KRint, KRound, KRowNumber, KRtrimmedLength, KSHA, KSHA1Digest, KSHA2, KSHA2Digest, KSafeAdd, KSafeConvertBytesToString, KSafeDivide, KSafeFunc, KSafeMultiply, KSafeNegate, KSafeSubtract, KSearch, KSearchIp, KSec, KSech, KSecond, KSeq1, KSeq2, KSeq4, KSeq8, KSessionUser, KSign, KSin, KSinh, KSkewness, KSortArray, KSoundex, KSoundexP123, KSpace, KSplit, KSplitPart, KSplitToTable, KSqrt, KStDistance, KStPoint, KStandardHash, KStarMap, KStartsWith, KStddev, KStddevPop, KStddevSamp, KStrPosition, KStrToDate, KStrToMap, KStrToTime, KStrToUnix, KString, KStringToArray, KStripNullValue, KStrtok, KStrtokToArray, KStruct, KStructExtract, KStuff, KSubstring, KSubstringIndex, KSum, KSystimestamp, KTan, KTanh, KTime, KTimeAdd, KTimeDiff, KTimeFromParts, KTimeSlice, KTimeStrToDate, KTimeStrToTime, KTimeStrToUnix, KTimeSub, KTimeToStr, KTimeToTimeStr, KTimeToUnix, KTimeTrunc, KTimestamp, KTimestampAdd, KTimestampDiff, KTimestampFromParts, KTimestampLtzFromParts, KTimestampSub, KTimestampTrunc, KTimestampTzFromParts, KToArray, KToBase32, KToBase64, KToBinary, KToBoolean, KToChar, KToCodePoints, KToDays, KToDecfloat, KToDouble, KToFile, KToMap, KToNumber, KToVariant, KTransform, KTranslate, KTrim, KTrunc, KTry, KTryBase64DecodeBinary, KTryBase64DecodeString, KTryCast, KTryHexDecodeBinary, KTryHexDecodeString, KTryToDecfloat, KTsOrDiToDi, KTsOrDsAdd, KTsOrDsDiff, KTsOrDsToDate, KTsOrDsToDateStr, KTsOrDsToDatetime, KTsOrDsToTime, KTsOrDsToTimestamp, KTypeof, KUnhex, KUnicode, KUniform, KUnixDate, KUnixMicros, KUnixMillis, KUnixSeconds, KUnixToStr, KUnixToTime, KUnixToTimeStr, KUnnest, KUpper, KUtcDate, KUtcTime, KUtcTimestamp, KUuid, KVarMap, KVariance, KVariancePop, KVectorSearch, KWeek, KWeekOfYear, KWeekStart, KWidthBucket, KXMLElement, KXMLGet, KXMLTable, KXor, KYear, KYearOfWeek, KYearOfWeekIso, KZipf, K_ExplodeOuter, KWeightString}
