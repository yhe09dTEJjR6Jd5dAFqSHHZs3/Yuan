import sys
sys.dont_write_bytecode = True
import os
import math
import ast
import re
import json
import time
import uuid
import queue
import shutil
import signal
import sqlite3
import hashlib
import heapq
import tempfile
import threading
import subprocess
import platform
import configparser
import errno
import types
import copy
import zipfile
import stat
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path
from collections import deque
from contextlib import contextmanager, nullcontext
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps

APP_NAME = "Yuan"
STATE_NAME = f".{APP_NAME.lower()}-native-audio-14"
PYTHON_REQUIREMENT = ">=3.10"
PACKAGES = (
    "numpy>=1.26", "sounddevice>=0.5", "soundfile>=0.13", "soxr>=0.5", "psutil>=5.9", "certifi>=2024", "cryptography>=44"
)
TEXT = {
    "native_bootstrap": ("首次准备 · 自动创建运行环境与本地音频模型", "First setup · automatically preparing the runtime and local audio model"),
    "native_model_created": ("已按设备资源创建 Yuan 模型 · 尚未训练", "Yuan model created for this device · not trained yet"),
    "native_learning": ("自建模型学习中", "Native model learning"),
    "native_unverified": ("对话能力尚未通过验证", "Conversation ability is not yet verified"),
    "native_data_needed": ("缺少已验证的真人对话数据 · 公开音频学习不能替代对话训练与验收", "Verified human dialogue data missing · public-audio learning does not replace dialogue training and acceptance"),
    "dialogue_data_missing": ("对话未就绪 · 缺少已验证的真人对话数据", "Conversation not ready · verified human dialogue data missing"),
    "fault_resource": ("设备可用资源不足", "Insufficient available device resources"),
    "action_resource": ("可用资源不足 · 已暂停准备并保留状态", "Available resources are insufficient · preparation paused and state preserved"),
    "pip_bootstrap": ("自动引导安装工具 · 校验官方发布文件", "Bootstrapping installation tools · verifying official release files"),
    "release_restart_required": ("新交付结构已变更 · 重新启动后验收，当前服务保留", "New delivery architecture changed · restart for acceptance; current service preserved"),
    "reserved_storage": ("对话预留", "Conversation reserve"),
    "candidate_progress": ("候选指标改善", "Candidate metric improvements"),
    "serving_versions": ("已验收服务版本", "Accepted serving versions"),
    "contrast_pending": ("候选已隔离 · 对照证据或听测尚未通过", "Candidate isolated · contrast evidence or listening acceptance pending"),
    "stage_import_cryptography": ("验证发布签名组件", "Verifying publisher-signature component"),
    "publisher_required": ("由开发端提供已验收模型与训练资产", "Publisher must supply accepted model and training assets"),
    "learning_storage_pause": ("学习暂停 · 优先保留对话与检查点空间", "Learning paused · conversation and checkpoint space reserved"),
    "condition_check": ("运行条件检查", "Runtime condition check"),
    "retry_time_exhausted": ("累计启动执行预算已用尽", "Cumulative startup execution budget exhausted"),
    "retry_total_exhausted": ("启动失败总次数达到上限", "Total startup failure limit reached"),
    "retry_same_fault_exhausted": ("同类故障达到自动重试上限", "Automatic retry limit for this fault reached"),
    "retry_import_unchanged": ("单组件导入复检仍未完成，已停止重复启动", "Isolated import recheck also timed out; repeated startup stopped"),
    "retry_requires_change": ("等待运行条件变化或手动重新检查", "Waiting for changed conditions or a manual recheck"),
    "no_startup_task": ("当前没有正在执行的启动任务", "No startup task is currently running"),
    "waiting_conditions": ("仅检查恢复条件，不重复启动", "Checking recovery conditions only; startup is not repeated"),
    "retry_next_action": ("到时将从可用检查点恢复准备，不重复已校验的安装", "Preparation will resume from a usable checkpoint without repeating verified installation"),
    "runtime_inventory": ("校验已安装组件文件", "Verifying installed component files"),
    "runtime_install_reused": ("已安装组件通过复核，跳过重复安装", "Installed components reverified; redundant installation skipped"),
    "runtime_install_repair": ("安装文件复核未通过，修复候选运行环境", "Installed file verification failed; repairing the candidate runtime"),
    "runtime_import_diagnostic": ("正在执行单组件导入复检", "Running an isolated component import recheck"),
    "runtime_import_diagnostic_passed": ("单组件导入通过，继续完整预检", "Isolated import passed; continuing full preflight"),
    "import_profile": ("组件导入环境", "Component import environment"),
    "runtime_install_verified": ("安装文件校验通过", "Installed file verification passed"),
    "download_queued": ("等待下载", "Queued"),
    "download_retrying": ("保留进度，等待续传", "Partial retained; waiting to resume"),
    "download_files": ("已校验文件", "Verified files"),
    "download_parallel": ("并行下载", "Parallel downloads"),
    "download_overall": ("下载合计", "Total downloaded"),
    "download_speed": ("合计速度", "Combined speed"),
    "download_current": ("当前文件", "Current file"),
    "download_estimate": ("预计剩余下载时间", "Estimated download time left"),
    "download_estimate_unknown": ("剩余下载时间待估算", "Download time remaining not yet known"),
    "download_campaign_remaining": ("下载预算剩余", "Remaining download budget"),
    "retry_transfer_exhausted": ("累计下载预算已用尽，已保留可续传部分", "Cumulative download budget exhausted; resumable data retained"),
    "task_running": ("准备任务执行中", "Preparation task running"),
    "engine_waiting": ("音频引擎尚未启动", "Audio engine not started yet"),
    "download_connecting": ("连接中", "Connecting"),
    "download_receiving": ("接收中", "Receiving"),
    "download_verifying": ("校验中", "Verifying"),
    "download_complete": ("下载项校验完成", "Download item verified"),
    "download_connection_budget": ("连接期限", "Connection deadline"),
    "download_attempts": ("下载尝试", "Download attempt"),
    "startup_resources": ("启动任务资源", "Startup task resources"),
    "not_measured": ("尚未检测", "Not measured yet"),
    "process_running": ("执行中", "Running"),
    "process_idle": ("未运行子进程", "No child process running"),
    "owned_source_found": ("已发现自有模型交付来源，尚待完整验收", "Owned model delivery source found; full acceptance still required"),
    "owned_source_absent": ("尚未发现自有模型交付来源；环境就绪不等于可对话", "No owned model delivery source found; runtime readiness does not imply conversation readiness"),
    "owned_source_invalid": ("自有模型来源配置需修正；运行环境准备仍继续", "Owned model source configuration needs correction; runtime preparation continues"),
    "open_fault": ("查看处理方法", "View recovery action"),
    "operation_begin": ("开始", "Started"),
    "operation_complete": ("完成", "Completed"),
    "operation_cancelled": ("取消", "Cancelled"),
    "operation_failed": ("失败", "Failed"),
    "fault_network": ("目标来源连接或读取失败", "Target-source connection or read failed"),
    "fault_timeout": ("阶段未在执行预算内完成", "Stage did not complete within its execution budget"),
    "fault_integrity": ("组件或证书校验失败", "Component or certificate verification failed"),
    "fault_storage": ("所选目录空间或写权限受限", "Workspace space or write access is restricted"),
    "fault_resolution": ("包源候选查询未得到可用结果", "Package-index candidate query yielded no usable result"),
    "fault_compatibility": ("运行组件或解释器不兼容", "Runtime component or interpreter is incompatible"),
    "fault_configuration": ("运行配置需要修正", "Runtime configuration needs correction"),
    "fault_engine": ("音频引擎未能继续运行", "Audio engine could not continue"),
    "fault_unknown": ("运行任务失败，原因见当前阻塞原因", "Runtime task failed; see the current blocker"),
    "startup_blocked": ("启动已暂停 · 请查看阻塞原因", "Startup paused · review the blocker"),
    "network_wait": ("等待网络恢复 · 不重复启动模型", "Waiting for network recovery · model restart paused"),
    "recheck": ("重新检查", "Recheck"),
    "recheck_requested": ("已请求重新检查", "Recheck requested"),
    "conditions_changed": ("运行条件已改变 · 重新检查", "Runtime conditions changed · checking again"),
    "network_restored": ("目标来源恢复可达 · 继续准备", "Target source reachable again · resuming preparation"),
    "retry_budget_exhausted": ("自动重试次数或执行预算已用尽 · 处理原因后点击「重新检查」", "Automatic retry count or execution budget exhausted · address the cause and select Recheck"),
    "action_network": ("检查目标来源、网络、代理或证书；连接恢复后在预算内继续", "Check the target source, network, proxy or certificates; resume within budget when reachable"),
    "action_timeout": ("阶段已超时；检查运行条件后重新检查", "Stage timed out; review runtime conditions, then recheck"),
    "action_storage": ("释放所选目录的磁盘空间或恢复写权限后重新检查", "Free workspace disk space or restore write access, then recheck"),
    "action_integrity": ("校验未通过；检查组件来源或证书，不会跳过校验", "Verification failed; check the component source or certificates; verification remains required"),
    "action_resolution": ("包源没有提供可确认的兼容候选；检查包源配置后重新检查", "No confirmed compatible candidate from the index; review index configuration and recheck"),
    "action_configuration": ("运行配置无效；修正所选目录中的配置后重新检查", "Runtime configuration invalid; correct the workspace configuration and recheck"),
    "action_compatibility": ("没有通过验证的解释器；检查组件和解释器兼容性后重新检查", "No interpreter passed verification; review component and interpreter compatibility, then recheck"),
    "action_unknown": ("已停止无变化的重复启动；查看当前阻塞原因后重新检查", "Unchanged restart attempts stopped; review the current blocker, then recheck"),
    "fault_record": ("故障记录", "Fault record"),
    "recovery_action": ("下一步", "Next action"),
    "recovery_attempts": ("同类失败", "Matching failures"),
    "campaign_remaining": ("启动计算预算剩余", "Remaining startup compute budget"),
    "runtime_network": ("已应用包源、代理与证书配置", "Package index, proxy and certificate settings applied"),
    "runtime_cached_failure": ("本轮不再重复验证未变化的失败环境", "Unchanged failed runtime will not be rechecked in this attempt"),
    "stage_import_psutil": ("导入资源监测组件", "Importing resource monitor"),
    "stage_import_certifi": ("导入证书组件", "Importing certificate component"),
    "stage_import_sounddevice": ("导入音频设备组件", "Importing audio device component"),
    "stage_import_soundfile": ("导入音频文件组件", "Importing audio file component"),
    "stage_import_soxr": ("导入重采样组件", "Importing resampler"),
    "stage_probe_compute": ("验证自有网络计算与音频读写", "Verifying owned-network compute and audio I/O"),
    "dependency_loaded": ("运行组件已导入", "Runtime component imported"),
    "evaluation_cleanup": ("清理未使用的听测缓存 · 发布资产保留", "Removing unused listening cache · release assets preserved"),
    "review_storage_pause": ("空间不足 · 暂停生成新听测，保留模型与历史", "Insufficient space · new listening generation paused, models and history preserved"),
    "model_configuration_selected": ("已选择自有模型结构 · 原训练资产已隔离保留", "Owned model architecture selected · previous training assets isolated and retained"),
    "validation_window": ("分段验证结果", "Window validation result"),
    "input_encoding": ("编码当前语句", "Encoding the current utterance"),
    "memory_encoding": ("编码音频记忆", "Encoding audio memory"),
    "model_persist": ("保存模型资产", "Persisting model assets"),
    "review_generation": ("生成独立听测音频", "Generating independent listening cases"),
    "candidate_prepare": ("准备学习候选", "Preparing the learning candidate"),
    "training_data": ("准备训练数据", "Preparing training data"),
    "training_compute": ("执行梯度更新", "Running the gradient update"),
    "generation_block": ("生成原生音频块", "Generating a native audio block"),
    "generation_block_close": ("结束音频生成", "Closing audio generation"),
    "stream_playback": ("流式回复与播放", "Streaming reply and playback"),
    "learning_action": ("处理训练与验收操作", "Processing learning and acceptance actions"),
    "ending_unknown": ("未知 · 不参与结束监督", "Unknown · no ending supervision"),
    "ending_complete": ("完整语句 · 自然结束", "Complete utterance · natural ending"),
    "ending_truncated": ("截断片段 · 尚未结束", "Truncated segment · not ended"),
    "user_ending": ("输入录音结束状态", "Input recording ending"),
    "assistant_ending": ("回复录音结束状态", "Reply recording ending"),
    "annotations_changed": ("标注已更新 · 候选与验证状态已重置", "Annotations updated · candidate and validation state reset"),
    "memory_fixed_pause": ("模型常驻内存或最小训练块预算不足 · 暂停学习并复查", "Model residency or minimum training block exceeds headroom · learning paused with rechecks"),
    "memory_reduced": ("已缩小训练窗口 · 对话版本不变", "Training window reduced · conversation release unchanged"),
    "deployment_action_required": ("缺少已验收的 Yuan 自有模型交付包，请由开发端补齐；不会用随机模型开放对话。", "The publisher must supply an accepted Yuan-owned model package; random models cannot enable conversation."),
    "serving_model": ("对话版本", "Serving"),
    "candidate_model": ("学习基线", "Learning"),
    "source_retry": ("单个音频源稍后重试 · 其他来源继续", "Retrying this audio source later · other sources continue"),
    "runtime_fallback": ("当前解释器不兼容 · 正在更换独立 Python", "Current interpreter is incompatible · trying another managed Python"),
    "runtime_candidate_skipped": ("暂时跳过已确认不兼容的解释器", "Temporarily skipping an interpreter with confirmed incompatibility"),
    "stage_owned_release": ("检查自有模型交付包", "Checking the owned model delivery package"),
    "owned_release_missing": ("尚未提供已验收自有模型 · 声学下载不会自动完成对话准备", "No accepted owned model supplied · acoustic downloads alone cannot enable conversation"),
    "owned_release_installing": ("正在获取并校验自有模型交付包", "Acquiring and verifying the owned model delivery package"),
    "owned_release_verifying": ("自有模型资产已导入 · 正在校验独立听测记录", "Owned model assets imported · validating independent listening acceptance"),
    "owned_release_export": ("导出已验收自有模型", "Export accepted owned model"),
    "owned_release_exported": ("自有模型交付包已导出", "Owned model delivery package exported"),
    "context_trimmed": ("历史原始音频已按预算裁剪 · 当前语句继续编码", "Historical raw audio trimmed to budget · current utterance remains encoded"),
    "input_incomplete": ("本句收音或编码不完整 · 已跳过回复，请重新说一次", "Utterance capture or encoding incomplete · reply skipped; please repeat"),
    'process_reaped': ('进程已回收', 'Process reclaimed'),
    'attempt': ('新的启动尝试', 'New startup attempt'),
    'previous_failure': ('上次尝试失败（历史记录）', 'Previous attempt failed (history)'),
    'supervision': ('阶段监护', 'Stage supervision'),
    'operation': ('运行阶段', 'Runtime stage'),
    'stage_threads': ('配置计算线程', 'Configuring compute threads'),
    'stage_import_numpy': ('导入数值组件', 'Importing numerical components'),
    'stage_import_torch': ('导入计算组件', 'Importing compute components'),
    'stage_resources': ('初始化资源组件', 'Initializing resource components'),
    'stage_store': ('检查本地音频存储', 'Checking local audio storage'),
    'stage_device': ('检测计算设备', 'Detecting compute device'),
    'stage_configuration': ('读取模型结构与执行预算', 'Reading model structure and execution budgets'),
    'stage_build': ('构建 Yuan 自建网络', 'Building the Yuan network'),
    'stage_restore': ('校验并恢复 Yuan 权重', 'Verifying and restoring Yuan weights'),
    'stage_release': ('校验对话验收记录', 'Validating conversation acceptance'),
    'stage_audio': ('初始化音频组件', 'Initializing audio components'),
    'stage_pipeline': ('检查模型前向音频链路', 'Checking the model forward audio pipeline'),
    'stage_learning': ('初始化学习与存档任务', 'Initializing learning and storage workers'),
    'engine_startup': ('等待引擎初始化完成', 'Waiting for engine initialization'),
    'audio_probe': ('枚举并校验音频设备', 'Enumerating and validating audio devices'),
    'audio_preflight': ('打开设备并验证双向回调', 'Opening devices and verifying input/output callbacks'),
    'audio_open': ('打开音频设备', 'Opening audio devices'),
    'audio_abort': ('停止音频流', 'Stopping audio streams'),
    'audio_close': ('关闭音频设备', 'Closing audio devices'),
    'capture_stop': ('结束收音并处理尾部', 'Stopping capture and processing trailing audio'),
    'capture_incomplete': ('收音任务未正常结束 · 尾部音频完整性未确认', 'Capture did not finish normally · trailing audio completeness unconfirmed'),
    'remaining': ('剩余期限', 'Time remaining'),
    'stage_timeout': ('阶段执行期限已到 · 正在回收本次进程', 'Stage deadline reached · reclaiming this process'),
    'monitor_unknown': ('资源活动未知 · 执行期限仍有效', 'Resource activity unknown · execution deadline remains active'),
    'learning_ready': ('学习链路就绪', 'Learning pipeline ready'),
    'stop_incomplete': ('对话已中断 · 未确认全部音频存档成功', 'Conversation interrupted · complete audio storage not confirmed'),
    'natural_end': ('模型自然结束', 'Model ended naturally'),
    "choose_audio": ("选择音频", "Choose audio"),
    "learning_setup": ("开发者：训练与验收", "Developer: training and acceptance"),
    "pair_import": ("添加真人音频配对", "Add a human-recorded audio pair"),
    "pair_submitted": ("配对已提交 · 正在完整校验后导入", "Pair submitted · validating before atomic import"),
    "user_file": ("用户原始音频", "Original user audio"),
    "assistant_file": ("真人回复音频", "Human reply audio"),
    "reviewer": ("标注／评审人", "Annotator / reviewer"),
    "recording_id": ("原始录音标识（同源必须相同）", "Original recording ID (same source, same ID)"),
    "user_speaker": ("用户说话人标识", "User speaker ID"),
    "assistant_speaker": ("回复说话人标识", "Reply speaker ID"),
    "license": ("使用授权／许可", "Usage authorization / license"),
    "split": ("数据划分", "Dataset split"),
    "human_confirm": ("确认：真人录音、不同说话人、回复对应输入且有使用授权", "Confirm: human recordings, different speakers, matching reply, and authorized use"),
    "review_open": ("开始独立听测", "Open independent listening review"),
    "review_submit": ("提交听测结果", "Submit listening review"),
    "review_pending": ("未判断", "Not reviewed"),
    "review_pass": ("通过", "Pass"),
    "review_fail": ("不通过", "Fail"),
    "understandable": ("能听懂", "Understandable"),
    "follows_prompt": ("遵循提示词", "Follows instructions"),
    "relevant_reply": ("回复相关", "Relevant reply"),
    "ends_normally": ("正常结束", "Ends normally"),
    "no_abnormal_audio": ("没有异常音频", "No abnormal audio"),
    "review_stale": ("听测任务已变化，请重新打开", "The listening task changed; reopen the review"),
    "review_saved": ("听测结果已保存 · 通过全部校验后才会启用", "Review saved · activation requires all checks to pass"),
    "review_missing": ("尚无待听测候选 · 请查看缺失数据与训练状态", "No candidate awaits review; check missing data and training status"),
    "required_fields": ("请完整填写信息并确认真人录音与授权", "Complete all fields and confirm human recordings and authorization"),
    "review_incomplete": ("请填写评审人，并逐项评完全部案例", "Enter a reviewer and assess every item in every case"),
    "missing_data": ("缺少可信配对来源", "Missing trusted paired sources"),
    "training_required": ("尚未通过对话训练验证", "Dialogue training validation has not passed"),
    "storage_retry": ("存档暂时失败 · 正在有界重试", "Storage temporarily failed · bounded retry in progress"),
    "storage_failed": ("存档未成功 · 未保存片段已计入丢失统计", "Storage failed · unsaved segments counted as lost"),
    "storage_health": ("存储健康", "Storage health"),
    "saved": ("已存档", "Saved"),
    "lost": ("未保存", "Unsaved"),
    "title": ("让声音，直接抵达。", "A direct connection. Through voice."),
    "subtitle": ("提示词 + 处理后的音频 → AI 模型 → 音频", "Prompt + processed audio → AI model → audio"),
    "choose": ("选择文件夹", "Choose folder"),
    "folder": ("工作目录", "Workspace"),
    "empty_folder": ("选择后自动准备环境与模型", "Choose a folder to prepare everything automatically"),
    "prompt": ("提示词", "Instructions"),
    "default_prompt": ("你是 Yuan。请用自然、清晰的中文进行语音对话，认真回应用户。", "You are Yuan. Have a natural, clear spoken conversation in English and respond thoughtfully."),
    "start": ("开始对话", "Start conversation"),
    "stop": ("结束对话", "End conversation"),
    "confirm_title": ("确认结束", "Confirm ending"),
    "confirm_stop": ("确认结束本次对话？", "End this conversation?"),
    "confirm_close": ("确认结束本次对话并关闭 Yuan？", "End this conversation and close Yuan?"),
    "idle": ("等待开始", "Ready when you are"),
    "setup": ("正在自动准备", "Preparing automatically"),
    "runtime": ("准备独立运行环境", "Preparing an isolated runtime"),
    "download": ("下载并校验运行组件", "Downloading and verifying components"),
    "loading": ("装载 Yuan 自建音频模型", "Loading Yuan’s self-built audio model"),
    "checking": ("正在验证模型音频链路", "Checking the model audio pipeline"),
    "ready": ("一切就绪 · 可以开始对话", "Everything is ready · start a conversation"),
    "connecting": ("正在连接麦克风与扬声器", "Connecting the microphone and speaker"),
    "listening": ("我在听", "I'm listening"),
    "thinking": ("Yuan 正在直接生成语音回复", "Yuan is generating a direct audio reply"),
    "speaking": ("Yuan 正在说话", "Yuan is speaking"),
    "ending": ("正在结束本次对话", "Ending this conversation"),
    "ended": ("本次对话已结束", "Conversation ended"),
    "training": ("空闲学习 · 原生音频与对话目标", "Idle learning · native audio and dialogue objectives"),
    "training_acoustic": ("空闲学习 · 声学结构", "Idle learning · acoustic structure"),
    "training_dialogue": ("空闲学习 · 对话回复", "Idle learning · dialogue replies"),
    "validating": ("固定容量留出验证 · 候选权重尚未启用", "Bounded held-out validation · candidate not active"),
    "promoted": ("留出指标改善 · 学习权重已保存，对话版本不变", "Held-out metrics improved · learning weights saved, conversation release unchanged"),
    "kept": ("未通过全部验证 · 候选继续学习", "Validation not passed · candidate keeps learning"),
    "waiting_data": ("等待独立音频样本", "Waiting for independent audio samples"),
    "network": ("空闲联网获取公开音频", "Fetching public audio while idle"),
    "offline": ("公开音频源暂不可达 · 本地学习继续", "Public audio source unreachable · local learning continues"),
    "paused": ("学习暂缓 · 对话不受影响", "Learning paused · conversation remains available"),
    "retry": ("等待条件恢复 · 自动重试", "Waiting for recovery · retrying automatically"),
    "failure": ("当前条件未能完成准备", "Preparation could not complete under current conditions"),
    "devices": ("设备", "Devices"),
    "model": ("模型与链路", "Model and pipeline"),
    "audio": ("音频", "Audio"),
    "learning": ("持续学习", "Continuous learning"),
    "storage": ("本地存储", "Local storage"),
    "resources": ("实时资源", "Live resources"),
    "privacy": ("语音与提示词留在本地，不上传 · 空闲获取公开音频 · 自建模型直接生成音频，无 TTS", "Voice and prompts stay local · Public audio is fetched while idle · Native audio generation, no TTS"),
    "full_duplex": ("持续收音 · 生成和播放期间仍在听", "Always listening · including playback"),
    "barge_in": ("检测到新语音 · 已取消当前回复", "New speech detected · current reply cancelled"),
    "capture_saved": ("语音片段已保存", "Speech segment saved"),
    "capture_gap": ("收音发生缺口 · 已隔离受影响的训练样本", "Capture gap · affected training samples quarantined"),
    "capture_recover": ("收音设备恢复中 · 将自动重连", "Recovering audio capture · reconnecting automatically"),
    "echo_note": ("基础回声抑制 · 播放重叠片段不作训练目标；建议耳机", "Basic echo suppression · playback-overlap segments excluded from training; headphones recommended"),
    "capture_stopped": ("收音已停止 · 尾部语音已处理", "Capture stopped · trailing speech processed"),
    "capture_wait": ("正在完成尾部语音存档", "Finishing trailing speech storage"),
    "audio_policy": ("自适应音频参数", "Adaptive audio settings"),
    "candidate_reset": ("候选数值异常 · 已回退到有效存档", "Non-finite candidate · reverting to a valid checkpoint"),
    "pending": ("待处理片段", "Pending segments"),
    "excluded": ("不作训练目标", "Excluded from targets"),
    "closed": ("正在释放运行资源", "Releasing runtime resources"),
    "no_prompt": ("已自动填入默认提示词", "Default instructions filled in automatically"),
    "limited": ("资源长度边界已到 · 本轮已安全结束", "Resource length limit reached · turn ended safely"),
    "metrics_note": ("仅运行自有模型，不使用 TTS；候选指标改善与已验收服务版本分别展示。", "Own models only, without TTS; candidate metric improvements and accepted serving versions are shown separately."),
    "mic_missing": ("等待可用麦克风与扬声器", "Waiting for a microphone and speaker"),
    "history_full": ("存储空间不足 · 暂停保存新增历史", "Storage low · new history recording paused"),
    "input": ("输入", "Input"), "output": ("输出", "Output"),
    "seconds": ("秒", "seconds"), "turns": ("轮", "turns"),
    "local": ("本地", "Local"), "public": ("公开音频", "Public audio"),
    "steps": ("训练步数", "Training steps"),
    "accepted": ("已验证更新", "Validated updates"), "before": ("验证前", "Before"),
    "after": ("验证后", "After"), "wait": ("等待", "Waiting"),
    "available": ("可用", "Available"),
    "input_faults": ("收音异常", "Input faults"), "output_faults": ("播放异常", "Output faults"),
    "playback_gap": ("播放出现间断 · 收音继续", "Playback discontinuity · input remains active"),
    "checkpointing": ("保存候选学习进度", "Saving candidate learning progress"),
    "checkpoint_saved": ("学习进度已保存", "Learning progress saved"),
    "command_failed": ("控制通道中断 · 正在恢复", "Control channel interrupted · recovering"),
    "runtime_profile": ("运行环境与组件版本", "Runtime environment and component versions"),
    "elapsed": ("本次对话", "Session"),
    "queued": ("对话已排队 · 准备完成后自动开始", "Conversation queued · starts automatically when ready"),
    "prefill": ("提示词 + 音频直接进入 Yuan 模型", "Prompt + audio entering the Yuan model directly"),
    "generating": ("Yuan 模型直接生成音频", "Yuan model generating audio directly"),
    "activity": ("运行状态", "Activity"),
    "all_activity": ("展开记录", "View activity"),
    "older": ("较早", "Earlier"), "newer": ("较新", "Later"), "live": ("实时", "Live"),
    "runtime_message": ("运行组件反馈", "Runtime component feedback"),
    "journal_failure": ("运行状态保存失败 · 正在保留当前服务", "Runtime state save failed · preserving current service"),
    "backoff": ("自动恢复倒计时", "Automatic recovery countdown"),
    "pipeline_ok": ("音频链路", "Audio pipeline"),
    "history_context": ("使用历史提示词与音频上下文；生成音频不作目标", "Using historical prompts and audio context; generated audio is not a target"),
    "memory_pause": ("内存余量不足 · 暂停学习并自动复查", "Memory headroom is low · learning paused with automatic checks"),
    "source_skipped": ("公开音频暂不适用", "Public audio is not currently usable"),
    "recovered": ("运行条件已恢复", "Runtime conditions recovered"),
    "recording_resumed": ("运行记录写入已恢复", "Activity recording has resumed"),
    "resource_failure": ("资源监测暂不可用", "Resource monitoring is temporarily unavailable"),
    "learn_yield": ("学习已暂停，优先对话", "Learning yielded to conversation"),
    "started": ("对话已开始", "Conversation started"),
    "turn": ("本轮对话完成", "Turn completed"),
    "source": ("公开音频来源与许可", "Public audio source and license"),
    "pipeline": ("音频链路检查", "Audio pipeline check"),
    "progress": ("准备进度", "Preparation progress"),
    "metrics": ("资源监测", "Resource measurements"),
    "notice": ("运行提示", "Runtime notice"),
    "error": ("运行异常", "Runtime error"),
    "exit": ("音频进程已退出", "Audio process exited"),
    "disconnected": ("音频进程已断开", "Audio process disconnected"),
    "launcher_closed": ("工作目录资源已释放", "Workspace resources released"),
    "cold_start": ("自建模型冷启动 · 正在积累真实音频", "Self-built model cold start · collecting real audio"),
    "acoustic_only": ("声学指标已改善 · 尚无通过验收的对话模型", "Acoustic metrics improved · no accepted conversation model yet"),
    "dialogue_ready": ("对话能力已通过独立验证 · 可直接生成语音回复", "Dialogue ability passed independent validation · direct audio replies enabled"),
    "dialogue_wait": ("等待真实对话配对进入训练、验证与保护集", "Waiting for real dialogue pairs across training, validation and guard sets"),
    "experimental": ("实验音频模型 · 对话播放门槛尚未通过", "Experimental audio model · dialogue playback gate has not passed"),
    "collecting": ("录音已保存 · 结束对话后学习；对话播放门槛尚未通过", "Recording saved · learning resumes after the session; dialogue playback gate has not passed"),
    "training_unvalidated": ("候选持续训练 · 等待独立验证与保护样本", "Candidate is training · waiting for independent validation and guard samples"),
    "candidate_recovered": ("已恢复未启用的候选训练状态", "Inactive candidate training state restored"),
    "checkpoint_failure": ("模型存档无效 · 未装载损坏权重", "Invalid model checkpoint · damaged weights were not loaded"),
    "device_fallback": ("加速设备不可用 · 改用 CPU", "Accelerator unavailable · using CPU"),
    "sample_skipped": ("音频样本暂不可学习 · 已保留来源记录", "Audio sample is not learnable · provenance retained"),
    "source_catalog": ("公开音频目录", "Public audio catalog"),
    "dialogue_pair": ("音频配对记录", "Audio pair record"),
    "memory_recall": ("已检索跨会话音频记忆", "Cross-session audio memory recalled"),
    "memory_context_retained": ("已为历史记忆保留模型上下文", "Model context reserved for recalled memory"),
    "candidate_isolated": ("学习模型与对话版本隔离", "Learning isolated from serving"),
    "promotion_recovered": ("已完成中断前提交的模型更新", "Committed model update recovered after interruption"),
    "policy": ("自适应运行策略", "Adaptive runtime policy"),
    "preparing_model": ("环境已准备 · 等待模型验收完成", "Environment prepared · waiting for model acceptance"),
    "release_wait": ("对话训练指标已改善 · 等待独立听测验收", "Dialogue training improved · independent listening acceptance pending"),
    "release_required": ("尚无通过中英文听测验收的自建模型，暂不能开始对话", "No self-built model has passed Chinese and English listening acceptance yet"),
    "release_invalid": ("对话验收记录未通过校验 · 未开放播放", "Conversation acceptance record invalid · playback not enabled"),
    "release_loaded": ("已装载通过听测验收的对话版本", "Listening-accepted conversation release loaded"),
    "start_rejected": ("当前尚未满足对话条件", "Conversation requirements are not met"),
    "runtime_resolve": ("解析兼容的运行组件", "Resolving compatible runtime components"),
    "runtime_install": ("安装已校验的运行组件", "Installing verified runtime components"),
    "runtime_verify": ("校验组件版本、依赖与导入", "Checking component versions, dependencies and imports"),
    "runtime_wait": ("组件进程仍在运行 · 正在监测活动", "Component process running · monitoring activity"),
    "last_progress": ("距上次进展", "Since last progress"),
    "unknown_total": ("总量未知", "Total unknown"),
    "readiness": ("就绪检查", "Readiness checks"),
    "environment": ("环境", "Runtime"),
    "acceptance": ("对话验收", "Dialogue check"),
    "pair_candidate": ("相邻片段仅作候选 · 不作对话监督", "Adjacent segments are candidates, not dialogue supervision"),
    "dataset_import": ("导入已标注的本地音频数据", "Importing annotated local audio data"),
    "dataset_rejected": ("音频数据标注未通过校验", "Audio dataset annotations failed validation"),
    "storage_queue": ("待存档", "Awaiting storage"),
    "storage_overload": ("存档队列已满 · 已记录未保存片段，收音继续", "Storage queue full · unsaved segment reported, capture continues"),
    "inference_superseded": ("推理队列已更新 · 优先处理较新的语音", "Inference queue updated · newer speech takes priority"),
    "storage_flush": ("正在完成音频存档", "Finishing audio storage"),
    "source_paused": ("联网学习已让出资源", "Online learning yielded its resources"),
    "model_recovered": ("已恢复最近可用模型 · 原存档已保留", "Last usable model restored · original checkpoint retained"),
    "no_display": ("当前设备没有可用的图形界面。", "No graphical display is available on this device."),
}


def available_resources(psutil):
    memory = max(0, int(psutil.virtual_memory().available))
    cpus = max(1, os.cpu_count() or 1)
    try:
        affinity = psutil.Process().cpu_affinity()
        if affinity:
            cpus = min(cpus, len(affinity))
    except (AttributeError, OSError, NotImplementedError, psutil.Error):
        pass
    if sys.platform.startswith("linux"):
        roots = [Path("/sys/fs/cgroup")]
        try:
            for line in Path("/proc/self/cgroup").read_text().splitlines():
                hierarchy, controllers, name = line.split(":", 2)
                if hierarchy == "0" and not controllers and ".." not in Path(name).parts:
                    roots.append(Path("/sys/fs/cgroup") / name.lstrip("/"))
        except (OSError, ValueError):
            pass
        seen = set()
        for root in roots:
            for location in (root, *root.parents):
                if location in seen or Path("/sys/fs/cgroup") not in (location, *location.parents):
                    continue
                seen.add(location)
                for limit_file, used_file in (("memory.max", "memory.current"), ("memory/memory.limit_in_bytes", "memory/memory.usage_in_bytes")):
                    try:
                        limit = (location / limit_file).read_text().strip()
                        used = int((location / used_file).read_text().strip())
                        if limit.isdecimal():
                            memory = min(memory, max(0, int(limit) - used))
                    except (OSError, ValueError):
                        pass
                try:
                    quota, period = (location / "cpu.max").read_text().split()
                    if quota.isdecimal() and int(period) > 0:
                        cpus = min(cpus, max(1, math.ceil(int(quota) / int(period))))
                except (OSError, ValueError):
                    pass
    return {"memory": memory, "cpus": cpus}

def compatible_python(requirement, version):
    if not requirement:
        return True
    current = tuple(int(value) for value in version[:3])
    for item in str(requirement).split(","):
        match = re.fullmatch(r"\s*(>=|<=|==|!=|~=|>|<)\s*(\d+(?:\.\d+){0,2})(\.\*)?\s*", item)
        if match is None:
            return False
        operator, raw, wildcard = match.groups()
        parts = tuple(int(value) for value in raw.split("."))
        target = parts + (0,) * (3 - len(parts))
        if wildcard:
            if operator not in ("==", "!="):
                return False
            same = current[:len(parts)] == parts
            okay = same if operator == "==" else not same
        elif operator == "~=":
            if len(parts) < 2:
                return False
            prefix = parts[:-1]
            okay = current >= target and current[:len(prefix)] == prefix
        else:
            okay = {">=": current >= target, "<=": current <= target, ">": current > target, "<": current < target,
                    "==": current == target, "!=": current != target}[operator]
        if not okay:
            return False
    return True

def behavior_digest():
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    names = {"NativeAudio", "AudioSequence", "AudioEncoding", "AudioStore", "AudioLearner", "DatasetImporter", "SpeechGate", "SampleRing", "EchoSuppressor", "CaptureSession", "AudioPolicy", "AudioDevices", "StreamingPlayback", "Playback", "PlaybackResult", "ReplyCancel", "AnyCancel", "check", "safe_path", "can_converse", "behavior_digest", "supervised", "supervised_stream", "operation", "OutputReference", "StorageBudget", "ReleaseTrust", "AcceptancePolicy", "OwnedReleaseInstaller", "AudioPacketQueue"}
    nodes = []
    for node in tree.body:
        if getattr(node, "name", None) in names:
            nodes.append(ast.dump(node, include_attributes=False))
        elif isinstance(node, ast.ClassDef) and node.name == "Engine":
            for child in node.body:
                if isinstance(child, ast.FunctionDef) and child.name in ("conversation", "_memory_context", "check_pipeline"):
                    nodes.append(ast.dump(child, include_attributes=False))
    return payload_digest(nodes)


def payload_digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":")).encode("utf-8")).hexdigest()

def checked_json(path):
    try:
        with Path(path).open(encoding="utf-8") as stream:
            value = json.load(stream, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
        if not isinstance(value, dict):
            raise ValueError("Expected an object")
        return value
    except (OSError, ValueError, TypeError) as exc:
        raise YuanError("运行记录无法校验，原文件已保留 / Runtime record cannot be validated; original retained: " + str(Path(path).name)) from exc

def is_digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None

def can_converse(values):
    return all(values.get(key) is True for key in ("runtime_ready", "pipeline_ready", "audio_ready", "model_ready"))

class AnyCancel:
    def __init__(self, *events):
        self.events = tuple(value for value in events if value is not None)

    def is_set(self):
        return any(value.is_set() for value in self.events)

    def wait(self, timeout):
        deadline = time.monotonic() + max(0.0, timeout)
        while not self.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if self.events:
                self.events[0].wait(min(remaining, math.sqrt(tick())))
            else:
                time.sleep(min(remaining, math.sqrt(tick())))
        return self.is_set()

class Cancelled(Exception):
    pass

class YuanError(RuntimeError):
    pass

class YuanFault(YuanError):
    def __init__(self, message, *, category="unknown", code="runtime_failure", stage=None, **details):
        super().__init__(message)
        self.category = category
        self.code = code
        self.stage = stage
        self.details = details

class EngineFault(YuanFault):
    def __init__(self, message, *, category="engine", code="engine_failure", stage=None, **details):
        super().__init__(message, category=category, code=code, stage=stage, **details)

class InvalidDownload(YuanError):
    pass

class InvalidAudio(YuanError):
    pass

class IneligibleSample(YuanError):
    pass

class FlushResult(dict):
    def __bool__(self):
        return self.get("ok") is True

def tick():
    return max(sys.getswitchinterval(), time.get_clock_info("monotonic").resolution)

def check(cancel):
    if cancel is not None and cancel.is_set():
        raise Cancelled()

def tr(key, language="中文"):
    value = TEXT.get(key)
    if value:
        return value[language == "English"]
    return str(key)

def human_bytes(value):
    value = max(0.0, float(value))
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024

def human_duration(seconds):
    if not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds < 0:
        return "—"
    seconds = int(math.ceil(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02}m"

def local_message(value, language):
    if not isinstance(value, dict):
        return str(value)
    title = value.get(language) or value.get("中文") or value.get("English") or ""
    detail = value.get("detail")
    return "\n".join(str(part) for part in (title, detail) if part)

def safe_path(root, relative):
    root = Path(root).resolve()
    value = str(relative)
    if not value or "\\" in value or ":" in value or "\x00" in value:
        raise YuanError("目录路径无效 / Invalid workspace path")
    path = (root / value).resolve()
    if path == root or root not in path.parents:
        raise YuanError("拒绝访问工作目录以外的路径 / Refusing a path outside the workspace")
    return path

def database_path(root, name):
    path = safe_path(root, name)
    for suffix in ("", "-wal", "-shm", "-journal"):
        candidate = Path(root) / (name + suffix)
        if candidate.is_symlink():
            raise YuanError("拒绝使用链接数据库文件 / Refusing a linked database file")
        safe_path(root, name + suffix)
    return path

def sync_dir(path):
    if os.name != "nt":
        try:
            fd = os.open(str(path), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass

def atomic_json(path, payload, cancel=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".write-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        check(cancel)
        os.replace(name, path)
        sync_dir(path.parent)
    finally:
        if os.path.exists(name):
            os.unlink(name)

def read_json(path, default=None):
    try:
        with Path(path).open(encoding="utf-8") as stream:
            value = json.load(stream)
            return value if isinstance(value, dict) else ({} if default is None else default)
    except (OSError, ValueError, TypeError):
        return {} if default is None else default

def workspace_settings(state, language="中文", prompt=None):
    path = safe_path(state, "settings.json")
    values = checked_json(path) if path.exists() else {}
    language = values.get("language", language)
    prompt = values.get("prompt", tr("default_prompt", language) if prompt is None else prompt)
    if language not in ("中文", "English") or not isinstance(prompt, str):
        raise YuanFault("目录设置无效，原文件已保留 / Invalid workspace settings; original retained",
                        category="configuration", code="workspace_settings_invalid")
    return {"language": language, "prompt": prompt}

def digest_file(path, cancel=None, algorithm="sha256"):
    path = Path(path)
    value = hashlib.new(algorithm)
    with path.open("rb") as stream:
        while True:
            check(cancel)
            part = stream.read(os.statvfs(path.parent).f_bsize * 256 if hasattr(os, "statvfs") else 1024 * 1024)
            if not part:
                break
            value.update(part)
    return value.hexdigest()


def redact(value):
    if isinstance(value, dict):
        return {str(key): "<redacted>" if str(key).lower() in ("password", "token", "authorization", "api_key", "secret") else redact(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [redact(item) for item in value]
    if not isinstance(value, str):
        return value
    def clean(match):
        raw = match.group(0)
        try:
            parts = urllib.parse.urlsplit(raw)
            host = parts.netloc.rsplit("@", 1)[-1]
            return urllib.parse.urlunsplit((parts.scheme, ("<redacted>@" if "@" in parts.netloc else "") + host, parts.path,
                                           "<redacted>" if parts.query else "", ""))
        except ValueError:
            return "<redacted-url>"
    return re.sub(r"(?:https?|socks5h?)://[^\s\"'<>]+", clean, value, flags=re.IGNORECASE)

def failure_category(exc):
    seen = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, YuanFault) and current.category != "unknown":
            return current.category
        if isinstance(current, OSError) and (current.errno in (errno.ENOSPC, errno.EACCES, errno.EROFS, getattr(errno, "EDQUOT", -1)) or getattr(current, "winerror", None) in (5, 39, 112)):
            return "storage"
        if isinstance(current, urllib.error.HTTPError):
            return "configuration" if current.code in (401, 403, 407) else "network" if current.code in (408, 429) or current.code >= 500 else "resolution"
        current = current.__cause__ if current.__cause__ is not None else (None if current.__suppress_context__ else current.__context__)
    reason = concise_error(exc).lower()
    if any(word in reason for word in ("no space left", "out of space", "disk full", "permission denied", "access is denied", "read-only file system", "空间不足", "download exceeds the current resource budget")):
        return "storage"
    if any(word in reason for word in ("hash mismatch", "哈希", "摘要不匹配", "完整性", "certificate verify failed", "sslcertverificationerror", "artifact hashes", "versions differ from the lock", "artifact lock", "lock mismatch")):
        return "integrity"
    if any(word in reason for word in ("component execution deadline", "component progress", "stage deadline", "stage event channel", "阶段执行期限", "控制命令未按时", "control command timed out")):
        return "timeout"
    if any(word in reason for word in ("network connection deadline", "network read deadline", "timed out", "timeout", "temporary failure in name resolution", "getaddrinfo failed", "connection reset", "connection refused", "connection aborted", "network is unreachable", "retrying (retry", "max retries exceeded", "could not fetch url", "name or service not known")):
        return "network"
    if any(word in reason for word in ("requires a different python", "not a supported wheel", "unsupported python", "does not meet requirements", "dll load failed", "undefined symbol", "cannot open shared object file", "illegal instruction", "no module named ensurepip", "no module named 'ensurepip'", "no module named venv", "no module named 'venv'", "incompatible", "abi mismatch")):
        return "compatibility"
    if any(word in reason for word in ("no matching distribution found", "could not find a version that satisfies", "resolutionimpossible")):
        return "resolution"
    return "unknown"

def fault_details(exc, stage=None):
    category = failure_category(exc)
    details = dict(exc.details) if isinstance(exc, YuanFault) else {}
    if isinstance(exc, urllib.error.HTTPError):
        details.update(url=exc.geturl(), http_status=exc.code)
    if isinstance(exc, OSError) and (exc.errno in (errno.EACCES, errno.EPERM) or getattr(exc, "winerror", None) == 5):
        details["permission_denied"] = True
    return {"category": category, "code": getattr(exc, "code", category + "_failure"), "stage": getattr(exc, "stage", None) or stage,
            "details": details, "reason": concise_error(exc)}

def fault_event(exc, stage=None):
    return redact(fault_details(exc, stage))

def runtime_identity(state, entry=None):
    state = Path(state)
    pointer = read_json(state / "runtime-current.json")
    if entry is None and pointer.get("directory"):
        try:
            entry = read_json(safe_path(state, pointer["directory"]) / "lock.json")
        except YuanError:
            entry = None
    return payload_digest([platform.system(), platform.machine(), entry or {"host": sys.version, "executable": sys.executable}, os.cpu_count()])

class StartupControl:
    def __init__(self, cancel, deadline, clock=None, transfer_seconds=0.0):
        self.cancel, self._deadline, self.clock = cancel, deadline, clock or time.monotonic
        self.transfer_seconds = max(0.0, float(transfer_seconds))
        self._transfer_used = 0.0
        self._transfer_started = None
        self._transfer_depth = 0
        self.lock = threading.RLock()

    @property
    def transfer_used(self):
        with self.lock:
            active = max(0.0, self.clock() - self._transfer_started) if self._transfer_started is not None else 0.0
            return self._transfer_used + active

    @property
    def deadline(self):
        return self._deadline + min(self.transfer_seconds, self.transfer_used)

    def is_set(self):
        if self.cancel.is_set():
            return True
        with self.lock:
            if self._transfer_started is not None and self.transfer_used >= self.transfer_seconds:
                raise YuanFault("累计下载预算已用尽，已保留可续传部分 / Cumulative download budget exhausted; resumable data retained",
                                category="timeout", code="startup_transfer_deadline", stage="download")
            if self.clock() >= self.deadline:
                raise YuanFault("累计启动执行预算已用尽 / Cumulative startup execution budget exhausted", category="timeout", code="startup_campaign_deadline", stage="engine_startup")
        return False

    @contextmanager
    def transfer(self):
        check(self)
        if self.transfer_seconds <= 0:
            yield
            return
        with self.lock:
            if self._transfer_depth == 0:
                self._transfer_started = self.clock()
            self._transfer_depth += 1
        try:
            yield
        finally:
            with self.lock:
                self._transfer_depth -= 1
                if self._transfer_depth == 0:
                    self._transfer_used += max(0.0, self.clock() - self._transfer_started)
                    self._transfer_started = None

    def wait(self, seconds):
        check(self)
        with self.lock:
            remaining = self._deadline - self.clock() + min(self.transfer_seconds, self.transfer_used)
            if self._transfer_started is not None:
                remaining = min(remaining, self.transfer_seconds - self.transfer_used)
        self.cancel.wait(max(0.0, min(seconds, remaining)))
        return self.is_set()

class TransferBudget:
    def __init__(self, initial, maximum, idle, margin=2.0, clock=None):
        self.clock = clock or time.monotonic
        self.started = self.clock()
        self.deadline = self.started + initial
        self.hard_deadline = self.started + max(initial, maximum)
        self.idle, self.margin = idle, max(1.0, margin)
        self.previous = None
        self.previous_at = self.started
        self.speed = None

    def advance(self, current, total):
        now = self.clock()
        if self.previous is not None and current > self.previous:
            elapsed = max(tick(), now - self.previous_at)
            measured = (current - self.previous) / elapsed
            weight = -math.expm1(-elapsed / max(tick(), self.idle))
            self.speed = measured if self.speed is None else self.speed + weight * (measured - self.speed)
            remaining = max(0, total - current) / self.speed if total is not None and self.speed > 0 else self.idle
            self.deadline = max(self.deadline, min(self.hard_deadline, now + max(self.idle, remaining * self.margin)))
        elif self.previous is not None and current < self.previous:
            self.speed = None
        if self.previous is None or current != self.previous:
            self.previous, self.previous_at = current, now
        return self.deadline

class TransferRate:
    def __init__(self, interval, clock=None):
        self.clock = clock or time.monotonic
        self.interval = max(tick(), interval)
        self.at = self.clock()
        self.pending = 0
        self.value = None

    def advance(self, amount):
        self.pending += max(0, amount)
        now = self.clock()
        elapsed = now - self.at
        if elapsed >= self.interval:
            measured = self.pending / elapsed
            weight = -math.expm1(-elapsed / (self.interval * 4))
            self.value = measured if self.value is None else self.value + weight * (measured - self.value)
            self.at, self.pending = now, 0
        return self.value

class DownloadStorage:
    def __init__(self, directory, reserve):
        self.directory, self.reserve = Path(directory), max(0, int(reserve))
        self.lock = threading.Lock()

    def write(self, stream, block):
        with self.lock:
            if shutil.disk_usage(self.directory).free < self.reserve + len(block):
                raise YuanFault("下载将占用保留空间，已暂停 / Download would consume reserved space; paused",
                                category="storage", code="download_disk_reserve", stage="download", reserve_bytes=self.reserve)
            if stream.write(block) != len(block):
                raise YuanFault("下载文件写入未完成 / Download file write was incomplete", category="storage", code="download_short_write", stage="download")

class DownloadLedger:
    def __init__(self, artifacts, phase_id, emit, interval, clock=None):
        self.clock = clock or time.monotonic
        self.emit, self.phase_id = emit, phase_id
        self.interval = max(tick(), interval)
        self.last_emit = -math.inf
        self.lock = threading.RLock()
        self.rate = TransferRate(self.interval, self.clock)
        self.rows = {item["sha256"]: {"artifact_id": item["sha256"], "file": item["filename"], "file_index": index + 1,
                                    "current": 0, "total": item.get("size"), "transfer_state": "download_queued", "verified": False}
                     for index, item in enumerate(artifacts)}

    def update(self, artifact_id, values, transferred=0, force=False):
        with self.lock:
            row = self.rows[artifact_id]
            row.update(values)
            speed = self.rate.advance(transferred)
            now = self.clock()
            if not force and now - self.last_emit < self.interval:
                return
            self.last_emit = now
            rows = list(self.rows.values())
            active = [value for value in rows if not value.get("verified") and value.get("transfer_state") != "download_queued"]
            selected = row if row in active or not active else active[0]
            total = sum(value["total"] for value in rows) if all(value.get("total") is not None for value in rows) else None
            current = sum(value.get("current", 0) for value in rows)
            completed = sum(bool(value.get("verified")) for value in rows)
            eta = 0.0 if completed == len(rows) else max(0, total - current) / speed if total is not None and speed and current < total else None
            snapshot = {**selected, "phase_id": self.phase_id, "key": "download", "unit": "bytes", "file_count": len(rows),
                        "files_completed": completed, "active_files": len(active), "overall_current": current, "overall_total": total,
                        "overall_speed": speed, "eta_seconds": eta,
                        "last_progress_at": max((value.get("last_progress_at", 0) for value in rows), default=0)}
            self.emit("progress", **snapshot)

class NetworkConfig:
    def __init__(self, state, source=None):
        self.state = Path(state).resolve()
        self.source = dict(os.environ if source is None else source)
        path = safe_path(self.state, "network.json")
        local = checked_json(path) if path.exists() else {}
        if not isinstance(local.get("inherit_system", True), bool):
            raise YuanFault("网络继承配置必须为布尔值 / Network inheritance must be boolean", category="configuration", code="network_config_invalid")
        values = {}
        if local.get("inherit_system", True):
            source = self.source
            home = Path(source.get("HOME") or source.get("USERPROFILE") or str(Path.home()))
            config = source.get("PIP_CONFIG_FILE")
            if not source.get("YUAN_NETWORK_CONFIGURED") and config != os.devnull:
                if os.name == "nt":
                    paths = [Path(source.get("PROGRAMDATA", "C:/ProgramData")) / "pip/pip.ini", home / "pip/pip.ini"]
                    if source.get("APPDATA"):
                        paths.append(Path(source["APPDATA"]) / "pip/pip.ini")
                    paths.append(Path(sys.prefix) / "pip.ini")
                else:
                    paths = [Path(root) / "pip/pip.conf" for root in source.get("XDG_CONFIG_DIRS", "/etc/xdg").split(os.pathsep)]
                    paths += [Path("/etc/pip.conf"), home / ".pip/pip.conf", Path(source.get("XDG_CONFIG_HOME", str(home / ".config"))) / "pip/pip.conf", Path(sys.prefix) / "pip.conf"]
                if config:
                    paths.append(Path(config).expanduser())
                for candidate in dict.fromkeys(paths):
                    if not candidate.is_file():
                        continue
                    parser = configparser.RawConfigParser(interpolation=None)
                    try:
                        with candidate.open(encoding="utf-8-sig") as stream:
                            parser.read_file(stream)
                    except (OSError, configparser.Error, UnicodeError) as exc:
                        raise YuanFault("无法读取 pip 网络配置 / Cannot read pip network settings", category="configuration", code="pip_config_invalid", file=str(candidate)) from exc
                    for section in ("global", "install"):
                        if parser.has_section(section):
                            for key in ("index-url", "extra-index-url", "proxy", "cert", "client-cert"):
                                if parser.has_option(section, key):
                                    values[key] = parser.get(section, key).strip()
            for key in ("index-url", "extra-index-url", "proxy", "cert", "client-cert"):
                value = source.get("PIP_" + key.upper().replace("-", "_"))
                if value:
                    values[key] = value
            values.setdefault("index-url", source.get("UV_DEFAULT_INDEX") or source.get("UV_INDEX_URL") or "https://pypi.org/simple")
            values.setdefault("extra-index-url", source.get("UV_EXTRA_INDEX_URL", ""))
            proxies = urllib.request.getproxies() if source == dict(os.environ) else {}
            values.setdefault("proxy", source.get("https_proxy") or source.get("HTTPS_PROXY") or source.get("all_proxy") or source.get("ALL_PROXY") or proxies.get("https", ""))
            values.setdefault("cert", source.get("REQUESTS_CA_BUNDLE") or source.get("SSL_CERT_FILE", ""))
            values.setdefault("client-cert", source.get("SSL_CLIENT_CERT", ""))
        for field, key in (("index_url", "index-url"), ("proxy", "proxy"), ("ca_file", "cert"), ("client_cert", "client-cert")):
            if field in local:
                if not isinstance(local[field], str):
                    raise YuanFault("网络配置值必须为字符串 / Network setting must be a string", category="configuration", code="network_config_invalid", field=field)
                values[key] = local[field].strip()
        if "extra_index_urls" in local:
            extra = local["extra_index_urls"]
            if not isinstance(extra, list) or not all(isinstance(value, str) for value in extra):
                raise YuanFault("附加包源配置必须为字符串列表 / Extra indices must be a list of strings", category="configuration", code="network_config_invalid")
            values["extra-index-url"] = " ".join(extra)
        self.index_urls = list(dict.fromkeys([values.get("index-url") or "https://pypi.org/simple", *values.get("extra-index-url", "").split()]))
        for url in self.index_urls:
            parsed = urllib.parse.urlsplit(url)
            if parsed.scheme != "https" or not parsed.hostname or any(character.isspace() for character in url):
                raise YuanFault("包源必须使用 HTTPS / Package indices must use HTTPS", category="configuration", code="insecure_package_index", url=url)
        self.proxy = values.get("proxy", "")
        self.proxy_explicit = "proxy" in local
        if self.proxy:
            parsed = urllib.parse.urlsplit(self.proxy)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                raise YuanFault("代理必须为 HTTP 或 HTTPS 地址 / Proxy must be an HTTP or HTTPS URL", category="configuration", code="proxy_config_invalid")
        self.ca_file = values.get("cert", "")
        self.client_cert = values.get("client-cert", "")
        for attr in ("ca_file", "client_cert"):
            value = getattr(self, attr)
            if value:
                candidate = Path(value).expanduser()
                if not candidate.is_absolute():
                    candidate = self.state / candidate
                if not candidate.is_file():
                    raise YuanFault("证书文件不可用 / Certificate file unavailable", category="configuration", code="certificate_file_missing", file=str(candidate))
                setattr(self, attr, str(candidate.resolve()))
        self.inherit = local.get("inherit_system", True)

    def apply(self, env):
        for key in tuple(env):
            if key.startswith("PIP_") or key.startswith("UV_") or key in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "SSL_CLIENT_CERT"):
                env.pop(key, None)
            elif (not self.inherit or self.proxy_explicit) and key.lower() in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
                env.pop(key, None)
        env.update(PIP_CONFIG_FILE=os.devnull, PIP_INDEX_URL=self.index_urls[0], UV_INDEX_URL=self.index_urls[0],
                   UV_NATIVE_TLS="1", YUAN_NETWORK_CONFIGURED="1")
        if len(self.index_urls) > 1:
            env["PIP_EXTRA_INDEX_URL"] = env["UV_EXTRA_INDEX_URL"] = " ".join(self.index_urls[1:])
        if self.proxy:
            env["PIP_PROXY"] = self.proxy
            for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
                env[key] = self.proxy
        if self.ca_file:
            for key in ("PIP_CERT", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE"):
                env[key] = self.ca_file
        if self.client_cert:
            env["PIP_CLIENT_CERT"] = env["SSL_CLIENT_CERT"] = self.client_cert
        return env

    def opener(self):
        import ssl
        context = ssl.create_default_context(cafile=self.ca_file or None)
        if self.client_cert:
            context.load_cert_chain(self.client_cert)
        handlers = [HTTPSOnly(), urllib.request.HTTPSHandler(context=context)]
        if self.proxy:
            handlers.append(urllib.request.ProxyHandler({"http": self.proxy, "https": self.proxy}))
        elif not self.inherit or self.proxy_explicit:
            handlers.append(urllib.request.ProxyHandler({}))
        passwords = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        for url in self.index_urls:
            parts = urllib.parse.urlsplit(url)
            if parts.username is not None:
                host = parts.netloc.rsplit("@", 1)[-1]
                passwords.add_password(None, parts.scheme + "://" + host, urllib.parse.unquote(parts.username), urllib.parse.unquote(parts.password or ""))
        handlers.append(urllib.request.HTTPBasicAuthHandler(passwords))
        return urllib.request.build_opener(*handlers)

    def summary(self):
        return redact({"indices": self.index_urls, "proxy": self.proxy or None, "ca_file": self.ca_file or "system", "client_cert": bool(self.client_cert)})

class AdaptivePolicy:
    locks = {}
    registry_lock = threading.Lock()

    def __init__(self, state):
        self.state = Path(state)
        self.path = safe_path(self.state, "policy.json")
        self.effective_path = safe_path(self.state, "policy-effective.json")
        self.lock_path = safe_path(self.state, "policy.lock")
        with self.registry_lock:
            self.lock = self.locks.setdefault(str(self.state.resolve()), threading.RLock())
        saved = read_json(self.path, {})
        self.overrides = saved.get("overrides", {})
        if not isinstance(self.overrides, dict):
            self.overrides = {}
        saved = read_json(self.effective_path, {})
        self.effective = saved.get("effective", {})
        self.measurements = saved.get("measurements", {})
        if not isinstance(self.effective, dict):
            self.effective = {}
        if not isinstance(self.measurements, dict):
            self.measurements = {}
        self.dirty = {}
        self.observations = {}

    def number(self, section, key, default, lower, upper):
        lower = float(lower)
        upper = max(lower, float(upper))
        with self.lock:
            section_overrides = self.overrides.get(section, {})
            value = section_overrides.get(key, default) if isinstance(section_overrides, dict) else default
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                value = default
            value = max(lower, min(upper, float(value)))
            self.effective.setdefault(section, {})[key] = value
            self.dirty.setdefault(section, {})[key] = value
        return value

    def integer(self, section, key, default, lower, upper):
        return int(round(self.number(section, key, default, lower, upper)))

    def ratio(self, section, key, default):
        return self.number(section, key, default, 0.0, 1.0)

    def measured(self, section, key, default):
        with self.lock:
            row = self.measurements.get(section, {}).get(key, {})
            value = row.get("mean", default) if isinstance(row, dict) else default
        return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0 else default

    def observe(self, section, key, value):
        value = float(value)
        if not math.isfinite(value) or value < 0:
            return
        horizon = self.number("measurement", "effective_observations", math.sqrt(max(1, os.cpu_count() or 1)) + 1, 1, 4096)
        decay = math.exp(-1 / horizon)
        with self.lock:
            old = self.measurements.setdefault(section, {}).get(key, {})
            count = max(0, int(old.get("count", 0)))
            prior = old.get("mean", value)
            weight = max(0.0, float(old.get("weight", min(count, horizon)))) * decay
            mean = (float(prior) * weight + value) / (weight + 1)
            variance = (float(old.get("variance", 0.0)) * weight + (value - prior) * (value - mean)) / (weight + 1)
            row = {"count": count + 1, "weight": weight + 1, "mean": mean, "variance": max(0.0, variance),
                   "maximum": max(value, float(old.get("maximum", 0.0)) * decay), "updated": time.time()}
            self.measurements[section][key] = row
            delta = self.observations.setdefault(section, {}).setdefault(key, {"count": 0, "factor": 1.0, "weight": 0.0, "total": 0.0, "maximum": 0.0, "squares": 0.0})
            delta["count"] += 1
            delta["factor"] *= decay
            delta["weight"] = delta["weight"] * decay + 1
            delta["total"] = delta["total"] * decay + value
            delta["squares"] = delta["squares"] * decay + value * value
            delta["maximum"] = max(value, delta["maximum"] * decay)

    @contextmanager
    def exclusive(self):
        self.state.mkdir(parents=True, exist_ok=True)
        wait = self.number("policy", "lock_timeout_seconds", 5.0, tick(), 60.0)
        deadline = time.monotonic() + wait
        while True:
            try:
                lease = WorkspaceLease(self.lock_path)
                break
            except YuanError:
                if time.monotonic() >= deadline:
                    raise YuanError("配置写锁超时 / Configuration write lock timed out")
                time.sleep(min(math.sqrt(tick()), max(0.0, deadline - time.monotonic())))
        try:
            yield
        finally:
            lease.close()

    def save(self):
        with self.lock:
            if not self.dirty and not self.observations:
                return
            with self.exclusive():
                payload = read_json(self.effective_path, {})
                effective = payload.setdefault("effective", {})
                measures = payload.setdefault("measurements", {})
                if not isinstance(effective, dict) or not isinstance(measures, dict):
                    raise YuanError("运行测量记录无效 / Invalid runtime measurement record")
                for section, values in self.dirty.items():
                    effective.setdefault(section, {}).update(values)
                for section, values in self.observations.items():
                    target = measures.setdefault(section, {})
                    for key, delta in values.items():
                        row = target.get(key, {})
                        count = int(row.get("count", 0))
                        weight = max(0.0, float(row.get("weight", 0.0))) * delta["factor"]
                        total_weight = weight + delta["weight"]
                        prior = float(row.get("mean", 0.0))
                        mean = (prior * weight + delta["total"]) / total_weight
                        second = ((float(row.get("variance", 0.0)) + prior * prior) * weight + delta["squares"]) / total_weight
                        target[key] = {"count": count + delta["count"], "weight": total_weight, "mean": mean,
                                       "variance": max(0.0, second - mean * mean),
                                       "maximum": max(float(row.get("maximum", 0.0)) * delta["factor"], delta["maximum"]), "updated": time.time()}
                if read_json(self.effective_path, {}) != payload:
                    atomic_json(self.effective_path, payload)
                self.effective, self.measurements = copy.deepcopy(effective), copy.deepcopy(measures)
                self.dirty.clear()
                self.observations.clear()
                current = read_json(self.path, {}).get("overrides", {})
                if isinstance(current, dict):
                    self.overrides = current

@contextmanager
def operation(emit, key, scope="startup", budget_seconds=None, stall_seconds=None, **details):
    operation_id = uuid.uuid4().hex
    started = time.monotonic()
    values = {"key": key, "operation_id": operation_id, "scope": scope, "pid": os.getpid(), "thread": threading.current_thread().name, **details}
    if budget_seconds is not None:
        values["budget_seconds"] = budget_seconds
    if stall_seconds is not None:
        values["stall_seconds"] = stall_seconds
    emit("operation", **values, outcome="begin")
    try:
        yield operation_id
    except BaseException as exc:
        emit("operation", **values, outcome="cancelled" if isinstance(exc, Cancelled) else "failed", elapsed=time.monotonic() - started, reason=concise_error(exc), fault=fault_event(exc, key))
        raise
    else:
        emit("operation", **values, outcome="complete", elapsed=time.monotonic() - started)

def supervised(key, scope="runtime"):
    def decorate(function):
        @wraps(function)
        def execute(self, *args, **kwargs):
            with operation(self.emit, key, scope):
                return function(self, *args, **kwargs)
        return execute
    return decorate

def supervised_stream(key, scope="runtime"):
    def decorate(function):
        @wraps(function)
        def execute(self, *args, **kwargs):
            iterator = function(self, *args, **kwargs)
            try:
                while True:
                    with operation(self.emit, key, scope):
                        try:
                            value = next(iterator)
                        except StopIteration:
                            return
                    yield value
            finally:
                with operation(self.emit, key + "_close", scope):
                    iterator.close()
        return execute
    return decorate

class StageSupervisor:
    def __init__(self, state, emit, clock=None, identity=None):
        self.policy = AdaptivePolicy(state)
        self.emit = emit
        self.clock = clock or time.monotonic
        self.path = safe_path(state, "supervision.json")
        self.fingerprint = identity or runtime_identity(state)
        saved = read_json(self.path, {})
        profiles = saved.get("profiles", {})
        self.profiles = profiles if isinstance(profiles, dict) else {}
        profile = self.profiles.get(self.fingerprint, {})
        self.history = profile.get("stages", {}) if isinstance(profile, dict) else {}
        if not isinstance(self.history, dict):
            self.history = {}
        self.minimum = self.policy.number("supervision", "minimum_seconds", 1.0, tick(), 60.0)
        self.ceiling = self.policy.number("supervision", "maximum_phase_seconds", 1800.0, self.minimum, 86400.0)
        self.initial = self.policy.number("supervision", "initial_phase_seconds", 120.0, self.minimum, self.ceiling)
        self.margin = self.policy.number("supervision", "history_margin", 4.0, 1.0, 32.0)
        self.period = self.policy.number("supervision", "heartbeat_seconds", 1.0, math.sqrt(tick()), 10.0)
        self.grace = self.policy.number("supervision", "shutdown_seconds", 60.0, self.minimum, self.ceiling)
        self.policy.save()
        self.active = {}
        self.last_report = -math.inf
        self.startup = None
        self.history_dirty = False
        self.last_history_write = -math.inf

    def budget(self, key, suggested=None):
        baseline = self.initial
        row = self.history.get(key, {})
        if isinstance(row, dict):
            measured = row.get("seconds")
            if isinstance(measured, (int, float)) and not isinstance(measured, bool) and math.isfinite(measured) and measured > 0:
                baseline = max(self.initial, measured * self.margin)
        if isinstance(suggested, (int, float)) and not isinstance(suggested, bool) and math.isfinite(suggested) and suggested > 0:
            baseline = max(baseline, suggested)
        return self.policy.number("supervision", key + "_seconds", baseline, self.minimum, self.ceiling)

    def start(self, pid, remaining=None):
        now = self.clock()
        total = self.policy.number("supervision", "startup_total_seconds", self.initial * 16, self.initial, 86400.0)
        if remaining is not None:
            total = min(total, max(tick(), remaining))
        self.startup = {"key": "engine_startup", "started": now, "deadline": now + total, "budget_seconds": total, "pid": pid, "operation_id": "startup"}
        self.active["bootstrap"] = {"key": "engine_startup", "started": now, "deadline": now + self.budget("engine_startup"), "pid": pid, "operation_id": "bootstrap"}
        self.policy.save()

    def record(self, key, outcome, seconds, fault=None):
        row = self.history.setdefault(key, {})
        if not isinstance(row, dict):
            row = self.history[key] = {}
        row["at"] = time.time()
        if outcome == "complete":
            previous = row.get("seconds", seconds)
            if not isinstance(previous, (int, float)) or not math.isfinite(previous) or previous <= 0:
                previous = seconds
            row["seconds"] = max(seconds, (previous + seconds) / 2)
            row["successes"] = int(row.get("successes", 0)) + 1
            row["consecutive_failures"] = 0
        elif outcome == "failed":
            row["failures"] = int(row.get("failures", 0)) + 1
            row["consecutive_failures"] = int(row.get("consecutive_failures", 0)) + 1
            row["last_failure"] = {"elapsed_lower_bound": seconds, **(fault or {})}
        self.history_dirty = True

    def observe(self, event):
        kind = event.get("type")
        if kind == "ready":
            self.startup = None
            self.active.pop("bootstrap", None)
            self.active.pop("handoff", None)
        elif kind == "operation":
            identifier, key = event.get("operation_id"), event.get("key")
            if not isinstance(identifier, str) or not identifier or not isinstance(key, str) or not key:
                return
            outcome = event.get("outcome")
            now = self.clock()
            if outcome == "begin":
                self.active.pop("bootstrap", None)
                self.active.pop("handoff", None)
                if identifier not in self.active:
                    budget = self.budget(key, event.get("budget_seconds"))
                    suggested = event.get("stall_seconds", budget)
                    if isinstance(suggested, bool) or not isinstance(suggested, (int, float)) or not math.isfinite(suggested) or suggested <= 0:
                        suggested = budget
                    stall = self.policy.number("supervision", key + "_stall_seconds", suggested, self.minimum, budget)
                    self.active[identifier] = {**event, "started": now, "deadline": now + budget, "budget_seconds": budget,
                                               "stall_seconds": stall, "stall_deadline": now + stall, "progress": -1}
            elif outcome == "progress":
                row, current = self.active.get(identifier), event.get("current")
                if row is not None and row["key"] == key and isinstance(current, (int, float)) and not isinstance(current, bool) and math.isfinite(current) and current > row["progress"]:
                    row["progress"] = current
                    row["stall_deadline"] = now + row["stall_seconds"]
            elif outcome in ("complete", "cancelled", "failed"):
                row = self.active.get(identifier)
                if row is None or row["key"] != key:
                    return
                self.active.pop(identifier)
                if outcome != "cancelled":
                    self.record(key, outcome, max(tick(), now - row["started"]), event.get("fault"))
                    self.flush_history(force=outcome == "failed")
            else:
                return
            if self.startup and not self.active:
                self.active["handoff"] = {"key": "engine_startup", "started": now, "deadline": now + self.budget("engine_startup"), "pid": self.startup["pid"], "operation_id": "handoff"}
            self.last_report = -math.inf

    def flush_history(self, force=False):
        now = self.clock()
        if not self.history_dirty or not force and now - self.last_history_write < self.period:
            return
        self.last_history_write = now
        try:
            profiles = read_json(self.path, {}).get("profiles", {})
            if not isinstance(profiles, dict):
                profiles = {}
            profiles[self.fingerprint] = {"stages": self.history, "at": time.time()}
            limit = self.policy.integer("supervision", "retained_profiles", 8, 1, 128)
            profiles = dict(sorted(profiles.items(), key=lambda item: item[1].get("at", 0) if isinstance(item[1], dict) else 0, reverse=True)[:limit])
            atomic_json(self.path, {"profiles": profiles})
            self.history_dirty = False
        except (OSError, YuanError) as exc:
            self.emit("notice", key="journal_failure", component="supervision_history", reason=concise_error(exc))

    def poll(self):
        now = self.clock()
        rows = list(self.active.values()) + ([self.startup] if self.startup else [])
        def deadline(row):
            return min(row["deadline"], row.get("stall_deadline", row["deadline"]))
        for row in sorted(rows, key=deadline):
            if now >= deadline(row):
                details = {"operation_id": row.get("operation_id"), "pid": row.get("pid"), "elapsed": now - row["started"],
                           "budget_seconds": row["deadline"] - row["started"],
                           "timeout_kind": "total" if now >= row["deadline"] else "stalled", "component": row.get("component")}
                fault = EngineFault("阶段执行期限已到 / Stage deadline reached", category="timeout", code="stage_deadline" if now >= row["deadline"] else "stage_stalled", stage=row["key"], **details)
                if not row.get("failure_recorded"):
                    row["failure_recorded"] = True
                    self.record(row["key"], "failed", now - row["started"], fault_event(fault))
                    self.flush_history(force=True)
                raise fault
        self.flush_history()
        if now - self.last_report >= self.period:
            self.last_report = now
            row = min(rows, key=deadline) if rows else None
            if row:
                self.emit("supervision", key=row["key"], operation_id=row.get("operation_id"), pid=row.get("pid"), elapsed=now - row["started"],
                          remaining_seconds=max(0.0, deadline(row) - now), total_remaining_seconds=max(0.0, row["deadline"] - now), total=None)
            else:
                self.emit("supervision", complete=True)

class WorkspaceLease:
    def __init__(self, path):
        self.stream = None
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            raise YuanError("拒绝使用链接锁文件 / Refusing a linked lock file")
        stream = path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                if path.stat().st_size == 0:
                    stream.write(b"\0")
                    stream.flush()
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.stream = stream
        except Exception:
            stream.close()
            raise YuanError("另一个 Yuan 正在使用此目录 / Another Yuan is using this workspace")

    def close(self):
        stream, self.stream = self.stream, None
        if stream is None:
            return
        try:
            if os.name == "nt":
                import msvcrt
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

def workspace_environment(workspace):
    workspace = Path(workspace).resolve()
    state = safe_path(workspace, STATE_NAME)
    cache = safe_path(state, "cache")
    values = {
        "HOME": state / "home", "USERPROFILE": state / "home",
        "APPDATA": state / "config" / "roaming", "LOCALAPPDATA": state / "config" / "local",
        "TORCH_HOME": cache / "torch", "TORCH_EXTENSIONS_DIR": cache / "torch-extensions",
        "TRITON_CACHE_DIR": cache / "triton", "TORCHINDUCTOR_CACHE_DIR": cache / "inductor", "CUDA_CACHE_PATH": cache / "cuda",
        "XDG_CACHE_HOME": cache / "xdg", "XDG_CONFIG_HOME": state / "config", "XDG_DATA_HOME": state / "data", "XDG_STATE_HOME": state / "state",
        "UV_CACHE_DIR": cache / "uv", "UV_PYTHON_INSTALL_DIR": state / "python", "UV_TOOL_DIR": state / "tools",
        "UV_TOOL_BIN_DIR": state / "bin", "UV_PYTHON_BIN_DIR": state / "bin", "PIP_CACHE_DIR": cache / "pip",
        "TMPDIR": state / "temp", "TEMP": state / "temp", "TMP": state / "temp",
    }
    env = NetworkConfig(state).apply(os.environ.copy())
    for key in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "PYTHONSTARTUP"):
        env.pop(key, None)
    for key, path in values.items():
        path = safe_path(state, path.relative_to(state).as_posix())
        path.mkdir(parents=True, exist_ok=True)
        env[key] = str(path)
    env.update({
        "YUAN_WORKSPACE": str(workspace), "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1",
        "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8", "PIP_NO_COMPILE": "1", "UV_COMPILE_BYTECODE": "0",
        "PIP_DISABLE_PIP_VERSION_CHECK": "1", "PIP_NO_INPUT": "1", "PIP_CONFIG_FILE": os.devnull, "UV_NO_MODIFY_PATH": "1", "UV_PYTHON_INSTALL_BIN": "0",
    })
    return env

def host_python():
    path = Path(sys.executable)
    if os.name == "nt" and path.name.lower() in ("pythonw.exe", "pythonw_d.exe"):
        console = path.with_name(path.name.lower().replace("pythonw", "python", 1))
        if console.is_file():
            return str(console)
    return str(path)

def runtime_python(path):
    return Path(path) / ("Scripts/python.exe" if os.name == "nt" else "bin/python")

def end_process(process, timeout=None):
    if process is None or process.poll() is not None:
        return
    grace = max(tick(), timeout if timeout is not None else max(1.0, math.sqrt(os.cpu_count() or 1)))
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           check=False, timeout=grace, creationflags=subprocess.CREATE_NO_WINDOW)
        else:
            os.killpg(process.pid, signal.SIGTERM)
    except (OSError, subprocess.TimeoutExpired):
        try:
            process.terminate()
        except OSError:
            pass
    try:
        process.wait(timeout=grace)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        if os.name != "nt":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except OSError:
        pass
    try:
        process.wait(timeout=grace)
    except subprocess.TimeoutExpired as exc:
        raise EngineFault("系统未能回收进程，已停止启动重试 / OS could not reclaim the process; startup retries stopped", code="process_reap_failed", pid=process.pid, process_retained=True) from exc

def spawn(command, env, **kwargs):
    options = {"start_new_session": True} if os.name != "nt" else {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW}
    return subprocess.Popen(command, env=env, cwd=env["YUAN_WORKSPACE"], **options, **kwargs)

def run_command(command, env, cancel, progress=None, heartbeat=None, heartbeat_seconds=None, stall_seconds=None, timeout_seconds=None, stage=None, events=None):
    check(cancel)
    process = spawn(command, env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, text=True, encoding="utf-8", errors="replace", bufsize=1)
    result = deque(maxlen=256)
    updates = queue.Queue(maxsize=max(16, (os.cpu_count() or 1) * 2))
    stopped = threading.Event()
    started = last_output = last_activity = time.monotonic()
    last_heartbeat = -math.inf
    period = max(tick(), heartbeat_seconds or math.sqrt(tick()))
    budget = timeout_seconds if timeout_seconds is not None else max(period, stall_seconds * 4 if stall_seconds else 120.0)
    if isinstance(budget, bool) or not isinstance(budget, (int, float)) or not math.isfinite(budget) or budget <= 0:
        end_process(process)
        process.stdout.close()
        raise ValueError("Invalid process deadline")
    deadline = started + budget
    previous_activity = None
    monitor = None
    psutil = sys.modules.get("psutil")
    if psutil is not None:
        try:
            monitor = psutil.Process(process.pid)
        except Exception:
            pass
    evidence = {}
    child_fault = None
    def read():
        try:
            while not stopped.is_set():
                line = process.stdout.readline(65536)
                if not line:
                    break
                while not stopped.is_set():
                    try:
                        updates.put(line.rstrip("\r\n"), timeout=math.sqrt(tick()))
                        break
                    except queue.Full:
                        pass
        except (OSError, ValueError):
            pass
    reader = threading.Thread(target=read, daemon=True, name="YuanComponentOutput")
    reader.start()
    try:
        while process.poll() is None or reader.is_alive() or not updates.empty():
            check(cancel)
            try:
                line = updates.get(timeout=min(period, math.sqrt(tick())))
            except queue.Empty:
                line = None
            if line is not None:
                result.append(line)
                last_output = time.monotonic()
                category = failure_category(YuanError(line))
                if category != "unknown":
                    evidence.setdefault(category, line)
                event = None
                if events is not None:
                    try:
                        candidate = json.loads(line)
                        if isinstance(candidate, dict) and isinstance(candidate.get("type"), str):
                            event = candidate
                    except (ValueError, TypeError):
                        pass
                if event is not None:
                    if event.get("type") == "error" and isinstance(event.get("fault"), dict):
                        child_fault = event["fault"]
                    events(event)
                elif progress:
                    progress(redact(line))
            now = time.monotonic()
            if now >= deadline and process.poll() is None:
                raise YuanFault("组件执行期限已到 / Component execution deadline reached", category="timeout", code="component_deadline", stage=stage,
                                command=[str(value) for value in command], pid=process.pid, elapsed=now-started, budget_seconds=budget, output=redact(list(result)))
            if now - last_heartbeat >= period:
                last_heartbeat = now
                activity = None
                if monitor is not None:
                    try:
                        processes = [monitor, *monitor.children(recursive=True)]
                        cpu_seconds = read_bytes = write_bytes = 0.0
                        for child in processes:
                            cpu = child.cpu_times()
                            cpu_seconds += cpu.user + cpu.system
                            try:
                                io = child.io_counters()
                                read_bytes += io.read_bytes
                                write_bytes += io.write_bytes
                            except (AttributeError, OSError, psutil.Error):
                                pass
                        activity = (round(cpu_seconds, 6), read_bytes, write_bytes)
                        if activity != previous_activity:
                            previous_activity = activity
                            last_activity = now
                    except (OSError, psutil.Error):
                        monitor = None
                if heartbeat:
                    heartbeat({"pid": process.pid, "elapsed": now - started, "output_idle": now - last_output, "activity_idle": now - last_activity,
                               "process_running": process.poll() is None, "process_activity": activity, "monitor_available": activity is not None,
                               "remaining_seconds": max(0.0, deadline - now), "budget_seconds": budget})
                if stall_seconds and now - last_output > stall_seconds and process.poll() is None:
                    raise YuanFault("组件没有可观察进展 / No observable component progress", category="timeout", code="component_stalled", stage=stage,
                                    command=[str(value) for value in command], pid=process.pid, idle_seconds=now-last_output, output=redact(list(result)))
        returncode = process.wait()
        if child_fault:
            raise YuanFault(child_fault.get("reason") or "运行预检失败 / Runtime probe failed", category=child_fault.get("category", "unknown"),
                            code=child_fault.get("code", "runtime_probe_failed"), stage=child_fault.get("stage") or stage, **child_fault.get("details", {}))
        if returncode:
            category = next((key for key in ("storage", "integrity", "network", "compatibility", "resolution") if key in evidence), "unknown")
            urls = re.findall(r"https://[^\s\"'<>]+", evidence.get("network", ""))
            raise YuanFault("运行组件未能准备完成 / Runtime preparation failed", category=category, code="package_command_failed", stage=stage,
                            command=[str(value) for value in command], pid=process.pid, returncode=returncode, output=redact(list(result)),
                            evidence=redact(evidence), url=urls[0].rstrip(").,;:") if urls else None)
        return "\n".join(result)
    finally:
        stopped.set()
        try:
            if process.poll() is None:
                end_process(process)
            if events is not None:
                events({"type": "process_reaped", "scope": "runtime_probe", "pid": process.pid, "returncode": process.returncode})
        finally:
            reader.join(timeout=max(period, math.sqrt(tick())))
            if not reader.is_alive():
                process.stdout.close()

class RuntimeInstaller:
    def __init__(self, workspace, cancel, emit):
        self.workspace, self.cancel, self.emit = Path(workspace), cancel, emit
        self.state = safe_path(workspace, STATE_NAME)
        self.env = workspace_environment(workspace)
        self.lock_path = safe_path(self.state, "runtime-lock.json")
        self.policy = AdaptivePolicy(self.state)
        self.network_config = NetworkConfig(self.state)
        self.network = Network(cancel, self.policy, self.network_config)
        self.heartbeat_seconds = self.policy.number("runtime", "heartbeat_seconds", 1.0, math.sqrt(tick()), max(1.0, math.sqrt(os.cpu_count() or 1)))
        self.stall_seconds = self.policy.number("runtime", "process_stall_seconds", self.network.timeout_ceiling * 4, self.network.timeout_ceiling, max(self.network.timeout_ceiling * 4, self.network.timeout_ceiling * (os.cpu_count() or 1)))
        self.download_attempts = self.policy.integer("runtime", "download_attempts", 3, 1, max(3, math.isqrt(os.cpu_count() or 1)))
        self.phase = "runtime"
        self.phase_id = uuid.uuid4().hex
        self.supervisor = StageSupervisor(self.state, self.emit)
        self.progress = {}
        self.last_raw = None
        self.reuse_cache = {}
        self.cached_checked = False
        self.cached_value = None
        self.verified_entry = None
        self.policy.save()
        self.emit("runtime_network", key="runtime_network", **self.network_config.summary())

    def stage(self, key):
        self.phase = key
        self.phase_id = uuid.uuid4().hex
        self.progress = {"phase_id": self.phase_id, "key": key, "current": 0, "total": None, "unit": "bytes", "last_progress_at": time.time()}
        self.last_raw = None
        self.emit("status", key=key, phase_id=self.phase_id)
        self.emit("progress", **self.progress)
        self.resources()

    def resources(self, **values):
        self.selected_executable = str(values.pop("executable", getattr(self, "selected_executable", host_python())))
        data = {"executable": self.selected_executable, "process_running": False, "monitor_available": False,
                "task_running": True, "task_kind": self.phase}
        try:
            disk = shutil.disk_usage(self.state)
            data.update(disk_free=disk.free, disk_total=disk.total)
        except OSError:
            pass
        self.emit("startup_resources", **{**data, **values})

    def report(self, line):
        line = line.strip()
        if not line:
            return
        raw = re.fullmatch(r"Progress ([0-9]+) of ([0-9]+)", line)
        if raw:
            current, total = int(raw[1]), int(raw[2])
            now = time.monotonic()
            speed = None
            if self.last_raw and current >= self.last_raw[1]:
                speed = (current - self.last_raw[1]) / max(tick(), now - self.last_raw[0])
            self.last_raw = (now, current)
            self.progress.update(current=current, total=total or None, speed=speed, last_progress_at=time.time())
        else:
            if "Downloading " in line or "Using cached " in line:
                self.last_raw = None
                self.progress.update(current=0, total=None, speed=None, file=line, detail=line)
            else:
                self.progress["detail"] = line
            self.progress["last_progress_at"] = time.time()
            self.emit("runtime_message", detail=line, stage=self.phase)
        self.emit("progress", **self.progress)

    def command(self, command, report=False):
        is_probe = any(flag in command for flag in ("--probe", "--probe-import", "--verify-install"))
        def receive(event):
            self.supervisor.observe(event)
            if event.get("type") not in ("error", "exit", "ready", "status"):
                values = {key: value for key, value in event.items() if key not in ("type", "attempt_id", "_sequence", "at", "event_sequence", "phase_id")}
                self.emit(event["type"], **values)
        def heartbeat(values):
            self.resources(executable=command[0], **values)
            if is_probe:
                self.supervisor.poll()
            self.emit("progress", **{**self.progress, **values})
        budget = self.supervisor.budget(self.phase, self.stall_seconds)
        env = dict(self.env)
        if is_probe:
            names = (command[-1],) if "--probe-import" in command else () if "--verify-install" in command else RUNTIME_MODULES
            budgets = {"stage_import_" + name: self.supervisor.budget("stage_import_" + name) for name in names}
            if "--probe" in command:
                budgets["stage_probe_compute"] = self.supervisor.budget("stage_probe_compute")
            if "--verify-install" in command:
                budgets["runtime_inventory"] = budget
            env["YUAN_IMPORT_BUDGETS"] = json.dumps(budgets)
            env["YUAN_IMPORT_IDENTITY"] = self.supervisor.fingerprint
            budget = max(budget, sum(budgets.values()) + self.supervisor.grace)
        if isinstance(self.cancel, StartupControl):
            check(self.cancel)
            budget = min(budget, max(tick(), self.cancel.deadline - time.monotonic()))
            env["YUAN_STARTUP_DEADLINE"] = str(self.cancel.deadline)
        self.supervisor.policy.save()
        began = time.monotonic()
        try:
            result = run_command(command, env, self.cancel, self.report if report else None, heartbeat=heartbeat,
                                 heartbeat_seconds=self.heartbeat_seconds, stall_seconds=None if is_probe else self.stall_seconds,
                                 timeout_seconds=budget, stage=self.phase, events=receive if is_probe else None)
            self.emit("runtime_message", stage=self.phase, command=[str(value) for value in command], returncode=0, elapsed=time.monotonic()-began)
            return result
        except YuanFault as exc:
            if exc.stage is None:
                exc.stage = self.phase
            raise
        finally:
            self.supervisor.flush_history(force=True)
            if is_probe:
                self.supervisor.active.clear()
                self.supervisor.startup = None
                self.emit("supervision", complete=True)
            self.resources(executable=command[0])

    def interpreter(self, request="host"):
        if request == "host":
            if sys.version_info[:2] < (3, 10):
                raise YuanError("当前 Python 版本不满足要求 / Host Python does not meet requirements")
            return host_python()
        executable = self._uv()
        self.command([str(executable), "--no-config", "python", "install", request], report=True)
        result = self.command([str(executable), "--no-config", "python", "find", "--managed-python", request]).strip().splitlines()
        if not result:
            raise YuanError("独立 Python 查找结果为空 / Managed Python lookup returned no result")
        resolved = Path(result[-1]).resolve()
        if Path(self.env["UV_PYTHON_INSTALL_DIR"]).resolve() not in resolved.parents or not resolved.is_file():
            raise YuanError("独立 Python 校验失败 / Managed Python validation failed")
        self.command([str(resolved), "-c", "import sys;assert sys.version_info >= (3,10);assert sys.implementation.name=='cpython'"])
        return str(resolved)

    def _platform_key(self, python):
        script = "import json,platform,sys,sysconfig;print(json.dumps([platform.system(),platform.machine(),sys.version_info[:2],sys.implementation.name,sysconfig.get_config_var('SOABI')],separators=(',',':')))"
        value = self.command([str(python), "-c", script]).strip()
        return hashlib.sha256(value.encode()).hexdigest(), value

    def _packages(self, python):
        script = "import importlib.metadata as m,json,re;v=[{'name':re.sub(r'[-_.]+','-',str(d.metadata['Name'])).lower(),'version':d.version} for d in m.distributions() if d.metadata.get('Name')];print(json.dumps(sorted(v,key=lambda x:x['name']),separators=(',',':')))"
        return json.loads(self.command([str(python), "-c", script]).strip())

    def _validate_entry(self, entry, platform_value):
        if not isinstance(entry, dict) or entry.get("platform") != platform_value or not isinstance(entry.get("artifacts"), list) or not entry["artifacts"]:
            raise YuanError("运行组件锁定记录不完整 / Runtime artifact lock is incomplete")
        packages = []
        for item in entry["artifacts"]:
            if not isinstance(item, dict) or not re.fullmatch(r"[a-z0-9]+(?:[-.][a-z0-9]+)*", str(item.get("name", ""))) or not re.fullmatch(r"[a-zA-Z0-9.!+_-]+", str(item.get("version", ""))) or not is_digest(item.get("sha256")):
                raise YuanError("运行组件缺少精确版本或 SHA256 / Runtime artifact lacks an exact version or SHA256")
            size = item.get("size")
            if size is not None and (isinstance(size, bool) or not isinstance(size, int) or size <= 0):
                raise YuanError("运行组件大小无效 / Invalid runtime artifact size")
            parsed = urllib.parse.urlparse(item.get("url", ""))
            filename = item.get("filename", "")
            if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or not re.fullmatch(r"[a-zA-Z0-9_.+!-]+\.whl", filename):
                raise YuanError("运行组件来源或文件名无效 / Invalid runtime artifact source or filename")
            if urllib.parse.unquote(Path(parsed.path).name) != filename:
                raise YuanError("运行组件来源与文件名不一致 / Runtime artifact source and filename mismatch")
            packages.append({"name": item["name"], "version": item["version"]})
        packages.sort(key=lambda item: item["name"])
        names = [item["name"] for item in packages]
        required = {re.split(r"[<>=!~\[]", item)[0].lower() for item in ("torch>=2.5", *PACKAGES)}
        if len(names) != len(set(names)) or not required.issubset(names) or packages != entry.get("packages"):
            raise YuanError("运行组件版本清单不完整或重复 / Runtime package manifest is incomplete or duplicated")
        return entry

    def _verify_hashes(self, previous, current):
        if set(previous) != set(current) or any(not hashes or not current[package] or hashes != current[package] for package, hashes in previous.items()):
            raise YuanError("运行组件哈希缺失或不匹配 / Runtime artifact hashes are missing or mismatched")

    def _entry_from_report(self, report, platform_value):
        if report.get("version") != "1" or not isinstance(report.get("install"), list):
            raise YuanError("运行组件解析报告版本不受支持 / Unsupported runtime resolution report version")
        artifacts = []
        for item in report["install"]:
            metadata = item.get("metadata", {})
            download = item.get("download_info", {})
            archive = download.get("archive_info", {})
            digest = archive.get("hashes", {}).get("sha256")
            url = download.get("url", "")
            if item.get("is_yanked"):
                raise YuanError("拒绝已撤回的运行组件 / Refusing a withdrawn runtime artifact")
            artifacts.append({"name": re.sub(r"[-_.]+", "-", str(metadata.get("name", ""))).lower(), "version": str(metadata.get("version", "")),
                              "url": url, "filename": urllib.parse.unquote(Path(urllib.parse.urlparse(url).path).name), "sha256": digest})
        artifacts.sort(key=lambda item: item["name"])
        entry = {"platform": platform_value, "packages": [{"name": item["name"], "version": item["version"]} for item in artifacts], "artifacts": artifacts}
        return self._validate_entry(entry, platform_value)

    def _installation_identity(self, python, entry):
        runtime = Path(python).parent.parent
        stamps = []
        for path in (Path(python), runtime / "pyvenv.cfg"):
            info = path.stat()
            stamps.append([str(path), info.st_size, info.st_mtime_ns, digest_file(path, self.cancel)])
        return payload_digest([payload_digest(entry), stamps])

    def _verify_installation(self, python, entry):
        self.supervisor.flush_history(force=True)
        self.supervisor = StageSupervisor(self.state, self.emit, identity=runtime_identity(self.state, entry))
        self.stage("runtime_inventory")
        self.command([str(python), "-B", str(Path(__file__).resolve()), "--verify-install", str(self.state)], report=True)

    def _probe(self, python, entry):
        self.supervisor.flush_history(force=True)
        self.supervisor = StageSupervisor(self.state, self.emit, identity=runtime_identity(self.state, entry))
        expected = {item["name"]: item["version"] for item in entry["packages"]}
        packages = self._packages(python)
        actual = {item["name"]: item["version"] for item in packages}
        if len(actual) != len(packages) or any(actual.get(name) != version for name, version in expected.items()) or set(actual) - set(expected) - {"pip", "setuptools", "wheel"}:
            raise YuanFault("运行环境版本与锁定记录不一致 / Runtime versions differ from the lock", category="integrity", code="installed_versions_mismatch", stage=self.phase)
        self.command([str(python), "-m", "pip", "check"], report=True)
        failure_path = Path(python).parent.parent / "probe-failure.json"
        identity = payload_digest([self._installation_identity(python, entry), digest_file(Path(__file__), self.cancel)])
        previous = read_json(failure_path)
        fault = previous.get("fault", {}) if previous.get("identity") == identity else {}
        component = str(fault.get("stage", "")).removeprefix("stage_import_")
        if fault.get("category") == "timeout" and component in RUNTIME_MODULES:
            self.emit("notice", key="runtime_import_diagnostic", component=component, previous_fault=fault)
            try:
                self.command([str(python), "-B", str(Path(__file__).resolve()), "--probe-import", str(self.state), component], report=True)
            except YuanFault as exc:
                if failure_category(exc) == "timeout":
                    exc.details.update(component=component, retry_blocker="retry_import_unchanged", diagnostic_mode="isolated_import")
                atomic_json(failure_path, {"identity": identity, "fault": fault_event(exc), "at": time.time()}, self.cancel)
                raise
            self.emit("notice", key="runtime_import_diagnostic_passed", component=component)
        try:
            self.command([str(python), "-B", str(Path(__file__).resolve()), "--probe", str(self.state)], report=True)
        except YuanFault as exc:
            atomic_json(failure_path, {"identity": identity, "fault": fault_event(exc), "at": time.time()}, self.cancel)
            raise
        failure_path.unlink(missing_ok=True)
        self.verified_entry = entry

    def _reuse(self, pointer, signature, lock):
        if not pointer or pointer.get("base_signature") != signature:
            return None
        runtime = safe_path(self.state, pointer["directory"])
        stamps = []
        for path in (runtime / "ready.json", runtime / "lock.json", runtime / "pyvenv.cfg", runtime_python(runtime)):
            try:
                info = path.stat()
                stamps.append((str(path), info.st_size, info.st_mtime_ns))
            except OSError:
                stamps.append((str(path), None, None))
        key = payload_digest([pointer, signature, lock, stamps])
        if key in self.reuse_cache:
            value, failure = self.reuse_cache[key]
            if failure is not None:
                self.emit("notice", key="runtime_cached_failure", reason=concise_error(failure))
                raise failure
            return value
        try:
            value = self._reuse_once(pointer, signature, lock)
        except Cancelled:
            raise
        except Exception as exc:
            self.reuse_cache[key] = (None, exc)
            raise
        self.reuse_cache[key] = (value, None)
        return value

    def _reuse_once(self, pointer, signature, lock):
        if not pointer or pointer.get("base_signature") != signature:
            return None
        runtime = safe_path(self.state, pointer["directory"])
        python = runtime_python(runtime)
        if not python.is_file():
            return None
        marker = checked_json(runtime / "ready.json")
        entry = checked_json(runtime / "lock.json")
        platform_key, platform_value = self._platform_key(python)
        self._validate_entry(entry, platform_value)
        digest = payload_digest(entry)
        expected = lock.get("platforms", {}).get(platform_key)
        if marker.get("lock") != digest or pointer.get("lock") != digest or marker.get("base_signature") != signature or expected != entry:
            raise YuanError("运行环境标记与锁定记录不一致 / Runtime marker and artifact lock mismatch")
        self._verify_hashes({item["name"]: item["sha256"] for item in expected["artifacts"]}, {item["name"]: item["sha256"] for item in entry["artifacts"]})
        self._verify_installation(python, entry)
        self.stage("runtime_verify")
        self._probe(python, entry)
        return python

    def _download(self, artifact, position, total_files, network=None, ledger=None, storage=None, cancel=None):
        network = network if network is not None else self.network
        cancel = cancel if cancel is not None else self.cancel
        target = safe_path(self.state, "runtime-artifacts/" + artifact["sha256"] + "/" + artifact["filename"])
        context = {"phase_id": self.phase_id, "artifact_id": artifact["sha256"], "file": artifact["filename"]}
        progress = {**context, "key": "download", "file_index": position, "file_count": total_files,
                    "current": 0, "total": artifact.get("size"), "unit": "bytes", "speed": None,
                    "transfer_state": "download_connecting", "last_progress_at": time.time()}
        def publish(transferred=0, force=False):
            if ledger is not None:
                ledger.update(artifact["sha256"], progress, transferred=transferred, force=force)
            else:
                self.progress = dict(progress)
                self.emit("progress", **self.progress)
        publish(force=True)
        def complete(path, cached=False):
            check(cancel)
            size = path.stat().st_size
            progress.update(current=size, total=size, cached=cached, verified=True, transfer_state="download_complete", last_progress_at=time.time())
            publish(force=True)
            self.emit("download_complete", key="download_complete", component="runtime_download", **context, cached=cached)
            self.policy.save()
            return path
        check(cancel)
        if target.is_file() and not target.is_symlink() and (artifact.get("size") is None or target.stat().st_size == artifact["size"]) and digest_file(target, cancel) == artifact["sha256"]:
            return complete(target, True)
        for attempt in range(self.download_attempts):
            check(cancel)
            previous = [None, -math.inf]
            rate = TransferRate(self.heartbeat_seconds)
            progress.update(download_attempt=attempt + 1, download_attempts=self.download_attempts, speed=None, verified=False)
            def state_changed(key, **values):
                progress.update(transfer_state="download_" + key, **values)
                publish(force=True)
            def report(done, total):
                now = time.monotonic()
                delta = max(0, done - previous[0]) if previous[0] is not None else 0
                previous[0] = done
                progress.update(current=done, total=total, speed=rate.advance(delta))
                if delta:
                    progress["last_progress_at"] = time.time()
                publish(transferred=delta)
                if now - previous[1] >= self.heartbeat_seconds:
                    self.resources()
                    previous[1] = now
            try:
                disk = shutil.disk_usage(self.state)
                reserve = storage.reserve if storage is not None else self.policy.integer("runtime", "disk_reserve_bytes", 0, 0, disk.total)
                budget = max(0, disk.free - reserve)
                part = target.with_name(target.name + ".partial")
                if part.is_file() and not part.is_symlink():
                    budget += part.stat().st_size
                path = network.download(artifact["url"], target, expected_size=artifact.get("size"), expected_hash=artifact["sha256"],
                                        max_bytes=budget, progress=report, events=state_changed, storage=storage)
                return complete(path)
            except Cancelled:
                raise
            except (OSError, YuanError) as exc:
                if isinstance(exc, YuanFault):
                    exc.details.update(artifact_id=artifact["sha256"], file=artifact["filename"])
                if attempt + 1 >= self.download_attempts or failure_category(exc) != "network":
                    raise
                network.note_failure(artifact["url"])
                delay = network.connection_timeout(artifact["url"]) * math.sqrt(attempt + 1)
                progress.update(transfer_state="download_retrying", speed=None)
                publish(force=True)
                self.emit("notice", key="retry", component="runtime_download", **context, attempt=attempt + 1, reason=concise_error(exc), retry_seconds=delay)
                if cancel.wait(delay):
                    raise Cancelled()
        raise YuanError("运行组件下载失败 / Runtime download failed")

    def _download_all(self, artifacts):
        if not artifacts:
            return []
        stop = threading.Event()
        cancel = AnyCancel(self.cancel, stop)
        connections = self.policy.integer("network", "pending_connections", 2, 1, 16)
        workers = self.policy.integer("runtime", "download_workers", min(connections, max(1, math.isqrt(os.cpu_count() or 1))), 1, min(connections, len(artifacts)))
        disk = shutil.disk_usage(self.state)
        storage = DownloadStorage(self.state, self.policy.integer("runtime", "disk_reserve_bytes", 0, 0, disk.total))
        ledger = DownloadLedger(artifacts, self.phase_id, self.emit, self.heartbeat_seconds)
        self.policy.save()
        output = [None] * len(artifacts)
        def fetch(index, artifact):
            check(cancel)
            network = Network(cancel, self.policy, self.network_config)
            return self._download(artifact, index + 1, len(artifacts), network=network, ledger=ledger, storage=storage, cancel=cancel)
        transfer = self.cancel.transfer() if isinstance(self.cancel, StartupControl) else nullcontext()
        with transfer:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="YuanDownload") as executor:
                futures = {}
                try:
                    for index, artifact in enumerate(artifacts):
                        check(cancel)
                        futures[executor.submit(fetch, index, artifact)] = index
                    for future in as_completed(futures):
                        check(cancel)
                        output[futures[future]] = future.result()
                except BaseException:
                    stop.set()
                    for future in futures:
                        future.cancel()
                    raise
        check(self.cancel)
        return output

    def _ensure_candidate(self, request):
        signature = payload_digest(["yuan-runtime-v2", PACKAGES, platform.system(), platform.machine(), PYTHON_REQUIREMENT, "torch>=2.5"])
        self.stage("runtime")
        lock = checked_json(self.lock_path) if self.lock_path.exists() else {"python_requirement": PYTHON_REQUIREMENT, "platforms": {}}
        if lock.get("python_requirement") != PYTHON_REQUIREMENT or not isinstance(lock.get("platforms"), dict):
            raise YuanError("运行环境锁定格式无效，未解除锁定 / Invalid runtime lock format; lock was not removed")
        current_path = self.state / "runtime-current.json"
        pointer = checked_json(current_path) if current_path.exists() else {}
        build_path = self.state / "runtime-build.json"
        build = checked_json(build_path) if build_path.exists() else {}
        if build.get("base_signature") == signature and build.get("interpreter") == request:
            runtime = safe_path(self.state, build["directory"])
        else:
            runtime = safe_path(self.state, "runtimes/" + uuid.uuid4().hex)
            runtime.parent.mkdir(parents=True, exist_ok=True)
            build = {"directory": runtime.relative_to(self.state).as_posix(), "base_signature": signature, "interpreter": request}
            atomic_json(build_path, build, self.cancel)
        python = runtime_python(runtime)
        if not python.is_file():
            self.create_environment(self.interpreter(request), runtime)
        else:
            try:
                self.command([str(python), "-c", "import sys;assert sys.version_info >= (3,10)"])
            except Cancelled:
                raise
            except Exception:
                runtime = safe_path(self.state, "runtimes/" + uuid.uuid4().hex)
                build = {"directory": runtime.relative_to(self.state).as_posix(), "base_signature": signature, "interpreter": request}
                atomic_json(build_path, build, self.cancel)
                self.create_environment(self.interpreter(request), runtime)
                python = runtime_python(runtime)
        help_text = self.pip_command(python, ["help", "install"])
        version_text = self.pip_command(python, ["--version"])
        match = re.match(r"pip ([0-9]+)(?:\.([0-9]+))?", version_text)
        pip_major = int(match[1]) if match else 0
        if pip_major < 23 or "--dry-run" not in help_text or "--report" not in help_text:
            self.command([str(python), "-m", "pip", "install", "--only-binary=:all:", "--no-deps", "--upgrade", "pip>=23"], report=True)
            help_text = self.pip_command(python, ["help", "install"])
        progress_args = ["--progress-bar", "raw" if re.search(r"\braw\b", help_text) else "off"]
        platform_key, platform_value = self._platform_key(python)
        entry = lock["platforms"].get(platform_key)
        if entry is None:
            self.stage("runtime_resolve")
            report_path = runtime / "resolve.json"
            self.command([str(python), "-m", "pip", "install", "--only-binary=:all:", "--dry-run", "--ignore-installed", "--report", str(report_path), *progress_args,
                          "--timeout", str(self.network.timeout_ceiling), "--retries", str(self.download_attempts), "torch>=2.5", *PACKAGES], report=True)
            entry = self._entry_from_report(checked_json(report_path), platform_value)
            lock["platforms"][platform_key] = entry
            atomic_json(self.lock_path, lock, self.cancel)
            report_path.unlink(missing_ok=True)
        else:
            self._validate_entry(entry, platform_value)
        if read_json(runtime / "lock.json") != entry:
            atomic_json(runtime / "lock.json", entry, self.cancel)
        requirements = runtime / "install-lock.txt"
        identity = self._installation_identity(python, entry)
        installed = build.get("installed") == {"identity": identity, "lock": payload_digest(entry)}
        if installed:
            try:
                self._verify_installation(python, entry)
            except Cancelled:
                raise
            except Exception as exc:
                if failure_category(exc) != "integrity":
                    raise
                installed = False
                self.emit("notice", key="runtime_install_repair", reason=concise_error(exc), fault=fault_event(exc))
            else:
                self.emit("notice", key="runtime_install_reused", executable=str(python))
        if not installed:
            build.pop("installed", None)
            build["stage"] = "preparing_install"
            atomic_json(build_path, build, self.cancel)
            self.stage("download")
            wheels = self._download_all(entry["artifacts"])
            content = "\n".join(item["name"] + " @ " + path.resolve().as_uri() + " --hash=sha256:" + item["sha256"] for item, path in zip(entry["artifacts"], wheels)) + "\n"
            with requirements.open("w", encoding="utf-8", newline="\n") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            self.stage("runtime_install")
            self.command([str(python), "-m", "pip", "install", "--no-index", "--no-deps", "--only-binary=:all:", "--no-compile", "--require-hashes", "--force-reinstall", "-r", str(requirements), *progress_args], report=True)
            build.update(stage="installed", installed={"identity": self._installation_identity(python, entry), "lock": payload_digest(entry)})
            atomic_json(build_path, build, self.cancel)
            self._verify_installation(python, entry)
        self.stage("runtime_verify")
        self._probe(python, entry)
        digest = payload_digest(entry)
        marker = {"base_signature": signature, "lock": digest, "platform": platform_key}
        atomic_json(runtime / "ready.json", marker, self.cancel)
        selected = {**marker, "directory": runtime.relative_to(self.state).as_posix()}
        if pointer:
            atomic_json(self.state / "runtime-previous.json", pointer, self.cancel)
        atomic_json(current_path, selected, self.cancel)
        build_path.unlink(missing_ok=True)
        requirements.unlink(missing_ok=True)
        self.emit("progress", key="runtime_verify", phase_id=self.phase_id, current=1, total=1, unit="checks", last_progress_at=time.time())
        self._cleanup_runtimes(selected)
        return python

    def _cleanup_runtimes(self, selected):
        protected = {selected["directory"]}
        for name in ("runtime-previous.json", "runtime-build.json"):
            value = read_json(self.state / name)
            if value.get("directory"):
                protected.add(value["directory"])
        root = safe_path(self.state, "runtimes")
        for path in root.iterdir():
            if path.is_dir() and not path.is_symlink() and path.relative_to(self.state).as_posix() not in protected and re.fullmatch(r"[0-9a-f]{32}", path.name):
                shutil.rmtree(path, ignore_errors=True)


    def bootstrap_pip(self, python):
        version = json.loads(self.command([str(python), "-c", "import json,sys;print(json.dumps(list(sys.version_info[:3])))"]))
        directory = safe_path(self.state, "tools/pip-bootstrap")
        directory.mkdir(parents=True, exist_ok=True)
        marker = read_json(directory / "release.json")
        if marker.get("file") and is_digest(marker.get("sha256")) and compatible_python(marker.get("requires_python"), version):
            candidate = safe_path(directory, marker["file"])
            if candidate.suffix == ".whl" and candidate.is_file() and digest_file(candidate, self.cancel) == marker["sha256"]:
                return candidate
        self.emit("notice", key="pip_bootstrap")
        catalog = self.network.json("https://pypi.org/pypi/pip/json")
        candidates = []
        for number, entries in catalog.get("releases", {}).items():
            if not re.fullmatch(r"[0-9]+(?:\.[0-9]+){1,2}", number):
                continue
            for entry in entries:
                name = entry.get("filename", "")
                url = urllib.parse.urlparse(entry.get("url", ""))
                checksum = entry.get("digests", {}).get("sha256")
                size = entry.get("size")
                if (entry.get("yanked") or not re.fullmatch(r"pip-[0-9.]+-py3-none-any\.whl", name)
                        or not compatible_python(entry.get("requires_python"), version) or not is_digest(checksum)
                        or isinstance(size, bool) or not isinstance(size, int) or size <= 0
                        or url.scheme != "https" or url.hostname != "files.pythonhosted.org" or url.username or url.password
                        or urllib.parse.unquote(Path(url.path).name) != name):
                    continue
                candidates.append((tuple(int(value) for value in number.split(".")), entry))
        if not candidates:
            raise YuanFault("没有兼容且可校验的安装工具 / No compatible verifiable installer is available",
                            category="compatibility", code="pip_bootstrap_unavailable", stage=self.phase)
        _, entry = max(candidates, key=lambda item: item[0])
        target = safe_path(directory, entry["filename"])
        available = StorageBudget(self.state).snapshot()["available_bytes"]
        maximum = self.policy.integer("runtime", "installer_download_bytes", 32 * 1024 ** 2, 1, max(1, available))
        with StorageBudget(self.state).reserve(entry["size"]):
            self.network.download(entry["url"], target, expected_size=entry["size"], expected_hash=entry["digests"]["sha256"], max_bytes=maximum)
        with zipfile.ZipFile(target) as archive:
            if "pip/__main__.py" not in archive.namelist() or archive.testzip() is not None:
                raise YuanError("安装工具归档无效 / Invalid installer archive")
        atomic_json(directory / "release.json", {"file": target.name, "sha256": entry["digests"]["sha256"],
                    "requires_python": entry.get("requires_python", ""), "version": entry["version"] if "version" in entry else target.name.split("-")[1]}, self.cancel)
        return target

    def pip_command(self, python, arguments, report=False):
        try:
            self.command([str(python), "-c", "import pip"])
        except Cancelled:
            raise
        except YuanFault as exc:
            if exc.code != "package_command_failed":
                raise
            archive = self.bootstrap_pip(python)
            script = "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));sys.argv[0]='pip';runpy.run_module('pip',run_name='__main__')"
            return self.command([str(python), "-c", script, str(archive), *arguments], report=report)
        return self.command([str(python), "-m", "pip", *arguments], report=report)

    def create_environment(self, python, runtime):
        try:
            self.command([str(python), "-m", "venv", "--without-pip", str(runtime)], report=True)
        except Cancelled:
            raise
        except YuanFault as exc:
            if exc.code != "package_command_failed" or exc.category in ("storage", "network", "integrity"):
                raise
            executable = self._uv()
            self.command([str(executable), "--no-config", "venv", "--python", str(python), str(runtime)], report=True)
        target = runtime_python(runtime)
        try:
            self.command([str(target), "-m", "ensurepip", "--upgrade"], report=True)
        except Cancelled:
            raise
        except YuanFault as exc:
            if exc.code != "package_command_failed" or exc.category in ("storage", "network", "integrity"):
                raise
            archive = self.bootstrap_pip(target)
            self.pip_command(target, ["install", "--no-index", "--no-deps", "--no-compile", str(archive)], report=True)

    def _uv(self):
        tools = self.state / "tools" / "uv"
        candidates = (tools / "bin" / "uv", tools / "Scripts" / "uv.exe", tools / "uv" / "uv", tools / "uv" / "uv.exe")
        executable = next((value for value in candidates if value.is_file() and not value.is_symlink()), None)
        if executable is None:
            tools.mkdir(parents=True, exist_ok=True)
            self.pip_command(host_python(), ["install", "--only-binary=:all:", "--no-deps", "--target", str(tools), "uv"], report=True)
            executable = next((value for value in candidates if value.is_file() and not value.is_symlink()), None)
        if executable is None:
            raise YuanError("无法自动取得兼容 Python 管理工具 / Compatible Python manager could not be provisioned")
        return executable

    def interpreter_candidates(self):
        if sys.version_info[:2] >= (3, 10) and sys.implementation.name == "cpython":
            yield "host"
        executable = self._uv()
        raw = self.command([str(executable), "--no-config", "python", "list", "--managed-python", "--output-format", "json", PYTHON_REQUIREMENT])
        rows = json.loads(raw)
        if not isinstance(rows, list):
            raise YuanError("Python 候选清单格式无效 / Invalid Python candidate catalog")
        versions = {}
        for row in rows:
            if not isinstance(row, dict) or row.get("implementation") != "cpython" or row.get("variant", "default") != "default":
                continue
            version = str(row.get("version", ""))
            if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
                continue
            values = tuple(int(value) for value in version.split("."))
            if values[:2] < (3, 10):
                continue
            key = row.get("key")
            if not isinstance(key, str) or not re.fullmatch(r"cpython-[a-zA-Z0-9_.+-]+", key):
                continue
            if not row.get("path") and urllib.parse.urlparse(str(row.get("url", ""))).scheme != "https":
                continue
            previous = versions.get(values[:2])
            if previous is None or values > previous[0]:
                versions[values[:2]] = (values, key)
        limit = self.policy.integer("runtime", "python_candidate_limit", max(1, len(versions)), 1, max(1, len(versions)))
        for _, key in sorted(versions.values(), reverse=True)[:limit]:
            yield key

    def failure_kind(self, exc):
        return failure_category(exc)

    def _cached_runtime(self):
        if self.cached_checked:
            return self.cached_value
        self.cached_checked = True
        signature = payload_digest(["yuan-runtime-v2", PACKAGES, platform.system(), platform.machine(), PYTHON_REQUIREMENT, "torch>=2.5"])
        current = read_json(self.state / "runtime-current.json")
        if not current:
            return None
        lock = checked_json(self.lock_path)
        try:
            self.cached_value = self._reuse(current, signature, lock)
            return self.cached_value
        except Cancelled:
            raise
        except Exception as exc:
            if failure_category(exc) not in ("integrity", "compatibility"):
                raise
            self.emit("notice", key="runtime", component="runtime_repair", reason=concise_error(exc), fault=fault_event(exc, self.phase))
            return None

    def ensure(self):
        self.stage("runtime")
        cached = self._cached_runtime()
        if cached is not None:
            return cached
        history_path = safe_path(self.state, "runtime-compatibility.json")
        history = read_json(history_path)
        records = history.get("candidates", {})
        if not isinstance(records, dict):
            records = {}
        interval = self.policy.number("runtime", "compatibility_recheck_seconds", 86400.0, 1.0, 30 * 86400.0)
        attempted = []
        for request in self.interpreter_candidates():
            check(self.cancel)
            key = payload_digest([request, sys.executable if request == "host" else None, sys.version if request == "host" else None,
                                  PACKAGES, "torch>=2.5", platform.system(), platform.machine(), self.network_config.index_urls])
            record = records.get(key, {})
            if record.get("failed_at", 0) + interval > time.time():
                attempted.append({"interpreter": request, "cached_failure": True, "reason": record.get("reason")})
                self.emit("notice", key="runtime_candidate_skipped", interpreter=request, reason=record.get("reason"), recheck_at=record["failed_at"] + interval)
                continue
            try:
                python = self._ensure_candidate(request)
                records.pop(key, None)
                atomic_json(history_path, {"candidates": records}, self.cancel)
                self.emit("runtime_profile", selected_interpreter=request, executable=str(python), verified=True)
                return python
            except Cancelled:
                raise
            except Exception as exc:
                kind = self.failure_kind(exc)
                reason = concise_error(exc)
                self.emit("runtime_failure", interpreter=request, classification=kind, stage=self.phase, reason=reason, fault=fault_event(exc, self.phase))
                if kind != "compatibility":
                    raise
                record = {"interpreter": request, "failed_at": time.time(), "reason": reason, "stage": self.phase}
                records[key] = record
                attempted.append(record)
                atomic_json(history_path, {"candidates": records}, self.cancel)
                self.emit("notice", key="runtime_fallback", interpreter=request, reason=reason)
        raise YuanFault("没有通过完整验证的 Python 候选 / No Python candidate passed complete verification", category="compatibility", code="python_candidates_exhausted", stage=self.phase, candidates=attempted)

class HTTPSOnly(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        if urllib.parse.urlparse(newurl).scheme != "https":
            raise YuanFault("拒绝非加密重定向 / Refused an insecure redirect", category="configuration", code="insecure_redirect")
        return super().redirect_request(request, fp, code, message, headers, newurl)

class Network:
    connection_registry = {}
    connection_registry_lock = threading.Lock()

    def __init__(self, cancel=None, policy=None, config=None):
        self.cancel = cancel
        self.config = config or (NetworkConfig(policy.state) if policy is not None else None)
        self.latency = 1.0
        self.policy = policy
        if policy is not None:
            self.timeout_floor = policy.number("network", "timeout_floor", 2.0, max(tick(), 0.1), max(2.0, math.sqrt(os.cpu_count() or 1) * 4))
            self.timeout_ceiling = policy.number("network", "timeout_ceiling", 30.0, self.timeout_floor, max(30.0, self.timeout_floor * max(4, os.cpu_count() or 1)))
        else:
            self.timeout_floor, self.timeout_ceiling = 2.0, 30.0
        self.cold_timeout = policy.number("network", "connect_cold_seconds", self.timeout_ceiling, self.timeout_floor, self.timeout_ceiling) if policy else self.timeout_ceiling
        self.connect_margin = policy.number("network", "connect_history_margin", 4.0, 1.0, 32.0) if policy else 4.0
        self.read_idle_seconds = policy.number("network", "read_idle_seconds", self.timeout_ceiling, self.timeout_floor, 86400.0) if policy else self.timeout_ceiling
        self.transfer_seconds = policy.number("network", "download_total_seconds", self.timeout_ceiling * 64, self.read_idle_seconds, 86400.0) if policy else self.timeout_ceiling * 64
        self.transfer_max_seconds = policy.number("network", "download_max_seconds", self.transfer_seconds * 4, self.transfer_seconds, 86400.0) if policy else self.transfer_seconds * 4
        self.transfer_margin = policy.number("network", "download_history_margin", 2.0, 1.0, 8.0) if policy else 2.0
        self.latency = self.cold_timeout / self.connect_margin
        self.host_budgets = {}
        self.host_lock = threading.Lock()
        if policy:
            policy.save()
        group = str(policy.state.resolve()) if policy is not None else "process"
        limit = policy.integer("network", "pending_connections", 2, 1, 16) if policy is not None else 2
        with self.connection_registry_lock:
            self.connection_gate = self.connection_registry.setdefault(group, threading.BoundedSemaphore(limit))

    @property
    def timeout(self):
        return max(self.timeout_floor, min(self.timeout_ceiling, self.latency * self.connect_margin))

    def origin_key(self, url):
        parts = urllib.parse.urlsplit(url)
        config = [self.config.proxy, self.config.ca_file, self.config.client_cert] if self.config else None
        return payload_digest([parts.scheme, parts.hostname, parts.port, config])

    def connection_timeout(self, url):
        key = self.origin_key(url)
        with self.host_lock:
            value = self.host_budgets.get(key)
        if value is None:
            measured = self.policy.measured("network_connect", key, -1.0) if self.policy else -1.0
            value = self.cold_timeout if measured < 0 else measured * self.connect_margin
        return max(self.timeout_floor, min(self.timeout_ceiling, value))

    def note_failure(self, url):
        budget = self.connection_timeout(url)
        with self.host_lock:
            self.host_budgets[self.origin_key(url)] = min(self.timeout_ceiling, max(self.cold_timeout, budget * math.sqrt(2)))

    def request(self, url, headers=None, method=None, deadline=None):
        if urllib.parse.urlparse(url).scheme != "https":
            raise YuanError("仅允许加密网络连接 / Only HTTPS connections are permitted")
        check(self.cancel)
        parts = urllib.parse.urlsplit(url)
        request_url = urllib.parse.urlunsplit((parts.scheme, parts.netloc.rsplit("@", 1)[-1], parts.path, parts.query, parts.fragment))
        request = urllib.request.Request(request_url, headers={"User-Agent": "Yuan-Audio/1.0 (local audio learning)", **(headers or {})}, method=method)
        before = time.monotonic()
        done = threading.Event()
        abandoned = threading.Event()
        result_box = []
        connect_budget = self.connection_timeout(url)
        deadline = min(deadline, before + connect_budget) if deadline is not None else before + connect_budget
        while not self.connection_gate.acquire(timeout=min(math.sqrt(tick()), max(tick(), deadline - time.monotonic()))):
            check(self.cancel)
            if time.monotonic() >= deadline:
                raise YuanFault("先前网络连接仍未退出 / Earlier network connections have not returned", category="network", code="network_connections_pending", url=url)
        def connect():
            result = None
            try:
                opener = self.config.opener() if self.config else urllib.request.build_opener(HTTPSOnly())
                result = opener.open(request, timeout=max(connect_budget, self.read_idle_seconds))
                if abandoned.is_set():
                    result.close()
                else:
                    result_box.append(result)
            except BaseException as exc:
                result_box.append(exc)
            finally:
                self.connection_gate.release()
                done.set()
                if abandoned.is_set() and result is not None:
                    result.close()
        try:
            check(self.cancel)
            threading.Thread(target=connect, daemon=True, name="YuanNetworkConnect").start()
        except BaseException:
            self.connection_gate.release()
            raise
        try:
            while not done.wait(math.sqrt(tick())):
                check(self.cancel)
                if time.monotonic() >= deadline:
                    raise YuanFault("网络连接期限已到 / Network connection deadline reached", category="network", code="network_connect_deadline", url=url, connect_seconds=connect_budget)
            check(self.cancel)
        except BaseException:
            abandoned.set()
            if result_box and not isinstance(result_box[0], BaseException):
                result_box[0].close()
            raise
        result = result_box[0]
        if isinstance(result, BaseException):
            if isinstance(result, urllib.error.HTTPError):
                raise result
            raise YuanFault("网络连接未完成 / Network connection failed", category=failure_category(result) if failure_category(result) != "unknown" else "network", code="network_connect_failed", url=url) from result
        elapsed = time.monotonic() - before
        self.latency = (self.latency + elapsed) / 2
        key = self.origin_key(url)
        with self.host_lock:
            self.host_budgets[key] = max(self.timeout_floor, min(self.timeout_ceiling, elapsed * self.connect_margin))
        if self.policy:
            self.policy.observe("network_connect", key, elapsed)
        if urllib.parse.urlparse(result.geturl()).scheme != "https":
            result.close()
            raise YuanFault("拒绝非加密重定向 / Refused an insecure redirect", category="configuration", code="insecure_redirect")
        return result

    def blocks(self, response, deadline=None):
        stopped = threading.Event()
        chunks = queue.Queue(maxsize=2)
        def send(value):
            while not stopped.is_set():
                try:
                    chunks.put(value, timeout=math.sqrt(tick()))
                    return
                except queue.Full:
                    pass
        def read():
            try:
                reader = getattr(response, "read1", response.read)
                while not stopped.is_set():
                    block = reader(1024 * 1024)
                    if not block:
                        break
                    send(block)
                send(None)
            except BaseException as exc:
                send(exc)
        thread = threading.Thread(target=read, daemon=True)
        thread.start()
        last_block = time.monotonic()
        try:
            while True:
                check(self.cancel)
                limit = deadline() if callable(deadline) else deadline
                if limit is not None and time.monotonic() >= limit:
                    raise YuanFault("单文件下载执行期限已到 / File transfer deadline reached", category="network", code="network_file_deadline", url=response.geturl() if hasattr(response, "geturl") else None)
                try:
                    block = chunks.get(timeout=math.sqrt(tick()))
                except queue.Empty:
                    if time.monotonic() - last_block >= self.read_idle_seconds:
                        raise YuanFault("网络读取期限已到 / Network read deadline reached", category="network", code="network_read_deadline", url=response.geturl() if hasattr(response, "geturl") else None)
                    continue
                last_block = time.monotonic()
                if block is None:
                    break
                if isinstance(block, BaseException):
                    if isinstance(block, Cancelled):
                        raise block
                    category = failure_category(block)
                    raise YuanFault("网络读取未完成 / Network read failed", category=category if category != "unknown" else "network", code="network_read_failed",
                                    url=response.geturl() if hasattr(response, "geturl") else None) from block
                yield block
        finally:
            stopped.set()
            if thread.is_alive():
                try:
                    import socket
                    raw = getattr(getattr(response, "fp", None), "raw", None)
                    connection = getattr(raw, "_sock", None)
                    if connection:
                        connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def json(self, url):
        payload = bytearray()
        with self.request(url) as response:
            for block in self.blocks(response):
                payload.extend(block)
                if len(payload) > 16 * 1024 * 1024:
                    raise YuanError("网络清单超过安全容量 / Network manifest exceeds safe capacity")
        check(self.cancel)
        return json.loads(payload)

    def download(self, url, target, expected_size=None, expected_hash=None, progress=None, max_bytes=None, algorithm=None, events=None, storage=None):
        budget = TransferBudget(self.transfer_seconds, self.transfer_max_seconds, self.read_idle_seconds, self.transfer_margin)
        deadline = budget.deadline
        target = Path(target)
        part = target.with_name(target.name + ".partial")
        check(self.cancel)
        if target.is_symlink() or part.is_symlink():
            raise InvalidDownload("拒绝使用链接指向的下载文件 / Refusing a linked download target")
        target.parent.mkdir(parents=True, exist_ok=True)
        algorithm = algorithm or "sha256"
        if algorithm not in ("sha1", "sha256"):
            raise InvalidDownload("不支持的完整性算法 / Unsupported integrity algorithm")
        if expected_size is not None and (isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size < 0):
            raise InvalidDownload("无效的下载大小 / Invalid download size")
        if max_bytes is not None and (isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 0):
            raise YuanError("无效的下载资源预算 / Invalid download resource budget")
        if expected_hash:
            expected_hash = str(expected_hash).lower()
            if len(expected_hash) != hashlib.new(algorithm).digest_size * 2 or any(c not in "0123456789abcdef" for c in expected_hash):
                raise InvalidDownload("无效的完整性摘要 / Invalid integrity digest")
        if max_bytes is not None and expected_size is not None and expected_size > max_bytes:
            raise YuanError("下载超过当前资源预算 / Download exceeds the current resource budget")
        if target.is_file():
            size = target.stat().st_size
            size_ok = (expected_size is None or size == expected_size) and (max_bytes is None or size <= max_bytes)
            hash_ok = not expected_hash or digest_file(target, self.cancel, algorithm) == expected_hash
            if size_ok and hash_ok:
                if progress:
                    progress(size, size)
                return target
        offset = part.stat().st_size if part.is_file() else 0
        if offset and (not expected_hash or max_bytes is not None and offset > max_bytes):
            part.unlink(missing_ok=True)
            offset = 0
        if offset and expected_hash and digest_file(part, self.cancel, algorithm) == expected_hash and (expected_size is None or offset == expected_size):
            check(self.cancel)
            os.replace(part, target)
            sync_dir(target.parent)
            if progress:
                progress(offset, offset)
            return target
        if expected_size is not None and offset >= expected_size:
            if offset == expected_size and part.is_file() and expected_hash and digest_file(part, self.cancel, algorithm) == expected_hash:
                check(self.cancel)
                os.replace(part, target)
                sync_dir(target.parent)
                if progress:
                    progress(offset, offset)
                return target
            part.unlink(missing_ok=True)
            offset = 0
        headers = {"Accept-Encoding": "identity"}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        if events:
            events("connecting", source=urllib.parse.urlsplit(url).hostname, connect_seconds=self.connection_timeout(url), read_idle_seconds=self.read_idle_seconds, transfer_seconds=self.transfer_seconds)
        try:
            response = self.request(url, headers, deadline=deadline)
        except urllib.error.HTTPError as exc:
            if exc.code != 416 or not offset:
                raise
            exc.close()
            part.unlink(missing_ok=True)
            offset = 0
            response = self.request(url, {"Accept-Encoding": "identity"}, deadline=deadline)
        with response:
            if events:
                events("receiving", source=urllib.parse.urlsplit(response.geturl()).hostname)
            status = getattr(response, "status", 200)
            if status not in (200, 206):
                raise YuanError(f"下载状态异常 / Unexpected download status: {status}")
            if response.headers.get("Content-Encoding", "identity").strip().lower() not in ("", "identity"):
                raise YuanError("下载编码与原始文件不匹配 / Download encoding differs from the original file")
            length = response.headers.get("Content-Length")
            if length is not None and not re.fullmatch(r"[0-9]+", length.strip()):
                raise YuanError("下载长度无效 / Invalid download content length")
            length = int(length) if length is not None else None
            response_end = None
            if status == 206:
                match = re.fullmatch(r"bytes ([0-9]+)-([0-9]+)/([0-9]+|\*)", response.headers.get("Content-Range", "").strip())
                if match is None:
                    raise YuanError("下载续传范围无效 / Invalid download content range")
                first, last = int(match[1]), int(match[2])
                complete = int(match[3]) if match[3] != "*" else None
                if first != offset or last < first or complete is not None and last >= complete:
                    raise YuanError("下载续传偏移不匹配 / Download resume offset mismatch")
                if length is not None and length != last - first + 1:
                    raise YuanError("下载范围与长度不匹配 / Download range and length mismatch")
                response_end = last + 1
                total = complete if complete is not None else expected_size
                if total is None:
                    raise YuanError("无法确认下载完整长度 / Cannot establish the complete download length")
            else:
                offset = 0
                total = length if length is not None else expected_size
            if expected_size is not None and total is not None and total != expected_size:
                raise InvalidDownload("下载大小与可信清单不匹配 / Download size differs from the trusted manifest")
            if max_bytes is not None and total is not None and total > max_bytes:
                raise YuanError("下载超过当前资源预算 / Download exceeds the current resource budget")
            written = offset
            budget.advance(written, total)
            if progress:
                progress(written, total)
            with part.open("ab" if offset else "wb") as stream:
                for block in self.blocks(response, deadline=lambda: budget.deadline):
                    check(self.cancel)
                    written += len(block)
                    limits = (expected_size, max_bytes, total, response_end)
                    if any(bound is not None and written > bound for bound in limits):
                        raise InvalidDownload("下载超过已声明大小 / Download exceeds its declared size")
                    if storage is not None:
                        storage.write(stream, block)
                    else:
                        if shutil.disk_usage(target.parent).free < len(block):
                            raise YuanError("所选目录空间不足 / The selected folder is out of space")
                        stream.write(block)
                    budget.advance(written, total)
                    if progress:
                        progress(written, total)
                stream.flush()
                os.fsync(stream.fileno())
        check(self.cancel)
        declared = expected_size if expected_size is not None else total
        if (declared is not None and written != declared) or (response_end is not None and written != response_end):
            raise YuanFault("下载提前结束，将保留已下载部分 / Download ended early; partial data retained",
                            category="network", code="network_incomplete", stage="download", url=url,
                            received_bytes=written, expected_bytes=declared, response_end=response_end)
        if events:
            events("verifying")
        if expected_hash and digest_file(part, self.cancel, algorithm) != expected_hash:
            part.unlink(missing_ok=True)
            raise InvalidDownload("文件完整性校验失败 / File integrity validation failed")
        check(self.cancel)
        os.replace(part, target)
        sync_dir(target.parent)
        return target



class StorageBudget:
    lock = threading.RLock()
    reservations = {}

    def __init__(self, state):
        self.state = Path(state).resolve()
        self.state.mkdir(parents=True, exist_ok=True)
        self.key = str(self.state)
        self.path = safe_path(self.state, "storage-budget.json")

    def configure(self, model, queue_bytes=0):
        page = sqlite3.connect(":memory:")
        try:
            page_bytes = int(page.execute("PRAGMA page_size").fetchone()[0])
        finally:
            page.close()
        parameters = getattr(model, "model_bytes", 0) or sum(value.numel() * value.element_size() for value in model.network.parameters())
        waveform = model.capacity() * model.np.dtype(model.np.float32).itemsize
        checkpoint = max(page_bytes, parameters * 4 + page_bytes)
        conversation = max(page_bytes, (max(queue_bytes, waveform * 2) + waveform * 2) * 2)
        atomic_json(self.path, {"model_identity": model.identity(), "checkpoint_bytes": checkpoint, "conversation_bytes": conversation})
        return self.snapshot()

    def snapshot(self, required=0, purpose="learning"):
        limits = read_json(self.path, {})
        checkpoint = max(0, int(limits.get("checkpoint_bytes", 0)))
        conversation = max(0, int(limits.get("conversation_bytes", 0)))
        free = shutil.disk_usage(self.state).free
        with self.lock:
            pending = self.reservations.get(self.key, 0)
        reserve = checkpoint if purpose == "conversation" else conversation if purpose == "checkpoint" else checkpoint + conversation
        return {"free_bytes": free, "reserved_bytes": reserve, "pending_bytes": pending,
                "checkpoint_bytes": checkpoint, "conversation_bytes": conversation,
                "available_bytes": max(0, free - reserve - pending),
                "ready": free >= reserve + pending + max(0, int(required)), "purpose": purpose}

    @contextmanager
    def reserve(self, required, purpose="learning"):
        required = max(0, int(required))
        with self.lock:
            result = self.snapshot(required, purpose)
            if not result["ready"]:
                raise OSError(errno.ENOSPC, "保留对话与检查点空间，已暂停新写入 / New write paused to preserve conversation and checkpoint space")
            self.reservations[self.key] = self.reservations.get(self.key, 0) + required
        try:
            yield result
        finally:
            with self.lock:
                remaining = max(0, self.reservations.get(self.key, 0) - required)
                if remaining:
                    self.reservations[self.key] = remaining
                else:
                    self.reservations.pop(self.key, None)

    def reclaim(self, store, cancel, required=0):
        removed = 0
        before = self.snapshot(required)
        if before["ready"]:
            return before
        key = store.setting("serving_memory_key", "")
        with store.transaction():
            store.db.execute("DELETE FROM memories WHERE model_key<>?", (key,))
        rows = store.db.execute("SELECT s.* FROM samples s JOIN groups g ON g.id=s.group_id WHERE g.origin='public' AND g.split='train' AND s.visits>0 AND NOT EXISTS(SELECT 1 FROM turn_pairs p WHERE (p.user_sample=s.id OR p.assistant_sample=s.id) AND p.verified=1) ORDER BY s.visits DESC,s.created").fetchall()
        with store.audio_files():
            for row in rows:
                check(cancel)
                if self.snapshot(required)["ready"]:
                    break
                path = safe_path(store.root, row["audio"])
                if path.is_symlink():
                    continue
                with store.transaction():
                    store.db.execute("DELETE FROM turn_pairs WHERE user_sample=? OR assistant_sample=?", (row["id"], row["id"]))
                    store.db.execute("DELETE FROM samples WHERE id=?", (row["id"],))
                if not store.db.execute("SELECT 1 FROM samples WHERE audio=?", (row["audio"],)).fetchone():
                    try:
                        size = path.stat().st_size
                        path.unlink()
                        removed += size
                    except FileNotFoundError:
                        pass
                    store.integrity_cache.pop(row["audio"], None)
        if removed:
            store.set_setting("storage_reclaimed_bytes", int(store.setting("storage_reclaimed_bytes", 0)) + removed)
        result = self.snapshot(required)
        result["reclaimed_bytes"] = removed
        return result

class AudioStore:
    file_lock = threading.RLock()
    memory_selection = "FROM samples s JOIN groups g ON g.id=s.group_id JOIN sessions x ON x.id=s.group_id WHERE g.origin='local' AND s.group_id<>? AND x.ended IS NOT NULL AND s.audio_status='ready' AND s.memory_eligible=1 AND s.count>1"

    @staticmethod
    def memory_reason(metadata, eligible=True, generated=False):
        for key in ("capture_gap", "playback_overlap", "interrupted", "discontinuous", "input_incomplete", "final"):
            if metadata.get(key):
                return key
        if not generated and not eligible:
            return "capture_quality"
        return ""


    def __init__(self, state, recover=True):
        self.root = safe_path(state, "memory")
        self.root.mkdir(parents=True, exist_ok=True)
        self.audio = safe_path(self.root, "audio")
        self.audio.mkdir(exist_ok=True)
        self.db = sqlite3.connect(str(database_path(self.root, "memory.sqlite3")), timeout=math.sqrt(os.cpu_count() or 1))
        self.db.row_factory = sqlite3.Row
        self.transaction_depth = 0
        self.integrity_cache = {}
        self.policy = AdaptivePolicy(state)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS groups(id TEXT PRIMARY KEY, split TEXT NOT NULL CHECK(split IN ('train','validation','guard','release')), origin TEXT NOT NULL, created REAL NOT NULL, source_key TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS group_source ON groups(source_key,split);
        CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, prompt TEXT NOT NULL, language TEXT NOT NULL, started REAL NOT NULL, ended REAL);
        CREATE TABLE IF NOT EXISTS samples(id TEXT PRIMARY KEY, group_id TEXT NOT NULL REFERENCES groups(id), role TEXT NOT NULL, task TEXT NOT NULL CHECK(task IN ('acoustic','generated_reply')), audio TEXT NOT NULL, seconds REAL NOT NULL, rate INTEGER NOT NULL, count INTEGER NOT NULL, cursor INTEGER NOT NULL DEFAULT 0, visits INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL, digest TEXT NOT NULL, file_sha256 TEXT NOT NULL, audio_status TEXT NOT NULL DEFAULT 'ready' CHECK(audio_status IN ('ready','retry','invalid')), retry_at REAL NOT NULL DEFAULT 0, read_failures INTEGER NOT NULL DEFAULT 0, audio_error TEXT NOT NULL DEFAULT '', generated INTEGER NOT NULL CHECK(generated IN (0,1)), learnable INTEGER NOT NULL CHECK(learnable IN (0,1)), dialogue_eligible INTEGER NOT NULL CHECK(dialogue_eligible IN (0,1)), memory_eligible INTEGER NOT NULL DEFAULT 0 CHECK(memory_eligible IN (0,1)), memory_exclusion TEXT NOT NULL DEFAULT '', exclusion TEXT NOT NULL DEFAULT '', metadata TEXT NOT NULL DEFAULT '{}');
        CREATE INDEX IF NOT EXISTS sample_group ON samples(group_id, created);
        CREATE INDEX IF NOT EXISTS sample_digest ON samples(digest);
        CREATE UNIQUE INDEX IF NOT EXISTS eligible_digest ON samples(digest) WHERE learnable=1 OR dialogue_eligible=1;
        CREATE INDEX IF NOT EXISTS sample_learning ON samples(task,learnable,visits,created);
        CREATE INDEX IF NOT EXISTS sample_memory ON samples(memory_eligible,audio_status,group_id,created);
        CREATE TABLE IF NOT EXISTS turn_pairs(id TEXT PRIMARY KEY, group_id TEXT NOT NULL REFERENCES groups(id), user_sample TEXT NOT NULL REFERENCES samples(id), assistant_sample TEXT NOT NULL REFERENCES samples(id), source TEXT NOT NULL, quality REAL NOT NULL, verified INTEGER NOT NULL CHECK(verified IN (0,1)), eligible INTEGER NOT NULL DEFAULT 1 CHECK(eligible IN (0,1)), eligibility_error TEXT NOT NULL DEFAULT '', evidence TEXT NOT NULL DEFAULT '{}', retry_at REAL NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', cursor INTEGER NOT NULL DEFAULT 0, visits INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL, UNIQUE(user_sample,assistant_sample));
        CREATE INDEX IF NOT EXISTS pair_learning ON turn_pairs(verified,visits,created);
        CREATE TABLE IF NOT EXISTS memories(sample_id TEXT NOT NULL REFERENCES samples(id) ON DELETE CASCADE, model_key TEXT NOT NULL, vector BLOB NOT NULL, dims INTEGER NOT NULL, created REAL NOT NULL, PRIMARY KEY(sample_id,model_key));
        CREATE INDEX IF NOT EXISTS memory_model ON memories(model_key,created);
        CREATE TRIGGER IF NOT EXISTS revoke_memory AFTER UPDATE OF memory_eligible,audio_status ON samples
        WHEN NEW.memory_eligible=0 OR NEW.audio_status<>'ready'
        BEGIN DELETE FROM memories WHERE sample_id=NEW.id; END;
        CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY, url TEXT NOT NULL, path TEXT, metadata TEXT NOT NULL, offset INTEGER NOT NULL DEFAULT 0, finished INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','ready','retry','quarantined','finished')), retry_at REAL NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '', created REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS source_schedule ON sources(finished,status,retry_at,created);
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)
        if recover:
            self.db.execute("UPDATE sessions SET ended=? WHERE ended IS NULL", (time.time(),))
            self.commit()
            for path in self.audio.glob("*.partial"):
                path.unlink(missing_ok=True)
            self.repair()

    def repair(self):
        for row in self.db.execute("SELECT * FROM samples").fetchall():
            try:
                safe_path(self.root, row["audio"]).stat()
            except (OSError, YuanError) as exc:
                self.audio_failure(row, exc)
        tracked = {row[0] for row in self.db.execute("SELECT audio FROM samples")}
        for path in self.audio.glob("*.flac"):
            if path.relative_to(self.root).as_posix() not in tracked and len(path.stem) == hashlib.sha256().digest_size * 2:
                path.unlink(missing_ok=True)

    def setting(self, key, default=None):
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        if not row:
            return default
        try:
            return json.loads(row[0])
        except (ValueError, TypeError):
            return default

    def set_setting(self, key, value):
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        if row and row[0] == encoded:
            return
        self.db.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (key, encoded))
        self.commit()

    def group(self, group_id, origin, forced=None, source_key=None):
        source_key = str(source_key or group_id)
        row = self.db.execute("SELECT split,source_key FROM groups WHERE id=?", (group_id,)).fetchone()
        if row:
            if source_key != group_id and row["source_key"] != source_key:
                if row["source_key"] != group_id or self.db.execute("SELECT 1 FROM samples WHERE group_id=?", (group_id,)).fetchone():
                    raise YuanError("同一来源组的原始录音标识发生变化 / Original recording identity changed within a group")
                related = self.db.execute("SELECT split FROM groups WHERE source_key=? LIMIT 1", (source_key,)).fetchone()
                split = related[0] if related else row["split"]
                if forced is not None and forced != split:
                    raise YuanError("同源录音不能分入不同数据集 / Recordings from one source cannot cross dataset splits")
                self.db.execute("UPDATE groups SET source_key=?,split=? WHERE id=?", (source_key, split, group_id))
                self.commit()
                return split
            if forced is not None and row["split"] != forced:
                raise YuanError("已有数据集划分不能在运行中改变 / An existing dataset split cannot change during operation")
            return row["split"]
        related = self.db.execute("SELECT split FROM groups WHERE source_key=? LIMIT 1", (source_key,)).fetchone()
        if related:
            split = related[0]
            if forced is not None and forced != split:
                raise YuanError("同源录音不能分入不同数据集 / Recordings from one source cannot cross dataset splits")
        elif forced in ("train", "validation", "guard", "release"):
            split = forced
        else:
            counts = dict(self.db.execute("SELECT split,count(DISTINCT source_key) FROM groups WHERE split<>'release' AND id IN (SELECT group_id FROM samples WHERE learnable=1 UNION SELECT group_id FROM turn_pairs WHERE verified=1) GROUP BY split"))
            empty = next((name for name in ("train", "validation", "guard") if not counts.get(name)), None)
            split = empty or min(("validation", "guard"), key=lambda name: counts.get(name, 0))
            if not empty and counts.get(split, 0) >= math.isqrt(sum(counts.values()) + 1):
                split = "train"
        self.db.execute("INSERT INTO groups(id,split,origin,created,source_key) VALUES(?,?,?,?,?)", (group_id, split, origin, time.time(), source_key))
        self.commit()
        return split

    def session(self, session_id, prompt, language):
        self.db.execute("INSERT INTO sessions VALUES(?,?,?,?,NULL)", (session_id, prompt, language, time.time()))
        self.commit()

    def end_session(self, session_id):
        self.db.execute("UPDATE sessions SET ended=? WHERE id=? AND ended IS NULL", (time.time(), session_id))
        self.commit()

    def add(self, group_id, role, waveform, rate, origin, cancel=None, generated=False, eligible=True, created=None, metadata=None, task="acoustic", sample_id=None, prepared=None):
        with self.audio_files(cancel):
            return self._add(group_id, role, waveform, rate, origin, cancel, generated, eligible, created, metadata, task, sample_id, prepared)

    def _add(self, group_id, role, waveform, rate, origin, cancel=None, generated=False, eligible=True, created=None, metadata=None, task="acoustic", sample_id=None, prepared=None):
        generated = bool(generated or (origin == "local" and role == "assistant"))
        task = "generated_reply" if generated else str(task)
        if task not in ("acoustic", "generated_reply"):
            raise YuanError("音频学习任务无效 / Invalid audio learning task")
        created = time.time() if created is None else float(created)
        if not math.isfinite(created):
            raise YuanError("音频时间无效 / Invalid audio timestamp")
        metadata = dict(metadata or {})
        encoded_metadata = json.dumps(metadata, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        own_preparation = prepared is None
        prepared = self.prepare_audio(waveform, rate, cancel, purpose="conversation" if origin == "local" else "learning") if prepared is None else prepared
        checksum = prepared["digest"]
        try:
            check(cancel)
            existing = self.db.execute("SELECT id FROM samples WHERE digest=? AND group_id=? AND role=? AND task=?", (checksum, group_id, role, task)).fetchone()
            if existing and origin == "public":
                return existing[0]
            sample_id = str(sample_id or uuid.uuid4().hex)
            with self.transaction():
                previous = self.db.execute("SELECT * FROM samples WHERE id=?", (sample_id,)).fetchone()
                if previous is not None:
                    if (previous["digest"] != checksum or previous["group_id"] != group_id or previous["role"] != role
                            or previous["task"] != task or previous["rate"] != prepared["rate"] or previous["count"] != prepared["count"]):
                        raise YuanError("音频片段标识与内容不一致 / Audio sample identity does not match its content")
                    if origin == "dataset" and (json.loads(previous["metadata"]) != metadata or previous["exclusion"] == "annotation_removed"):
                        reason = self.memory_reason(metadata, eligible, generated)
                        duplicate = self.db.execute("SELECT 1 FROM samples WHERE digest=? AND id<>? AND (learnable=1 OR dialogue_eligible=1)", (checksum, sample_id)).fetchone()
                        dialogue_eligible = bool(eligible) and not generated and not duplicate
                        learnable = dialogue_eligible and task == "acoustic"
                        exclusion = "generated" if generated else "capture_quality" if not eligible else "duplicate_audio" if duplicate else ""
                        self.db.execute("UPDATE samples SET metadata=?,created=?,cursor=0,visits=0,memory_eligible=?,memory_exclusion=?,learnable=?,dialogue_eligible=?,exclusion=? WHERE id=?",
                                        (encoded_metadata, created, int(not reason), reason, int(learnable), int(dialogue_eligible), exclusion, sample_id))
                        self.db.execute("DELETE FROM memories WHERE sample_id=?", (sample_id,))
                        self.db.execute("UPDATE turn_pairs SET eligible=0,verified=0,cursor=0,visits=0,retry_at=0,failures=0,error='' WHERE user_sample=? OR assistant_sample=?", (sample_id, sample_id))
                        self.set_setting("data_revision", uuid.uuid4().hex)
                    return sample_id
                self.group(group_id, origin, source_key=metadata.get("recording_id") or metadata.get("source") or group_id)
                duplicate = self.db.execute("SELECT 1 FROM samples WHERE digest=? AND generated=0 AND (learnable=1 OR dialogue_eligible=1)", (checksum,)).fetchone()
                learnable = bool(eligible) and not generated and not duplicate and task == "acoustic"
                dialogue_eligible = bool(eligible) and not generated and not duplicate
                exclusion = "generated" if generated else "capture_quality" if not eligible else "duplicate_audio" if duplicate else ""
                memory_reason = self.memory_reason(metadata, eligible, generated)
                self.db.execute("INSERT INTO samples(id,group_id,role,task,audio,seconds,rate,count,created,digest,file_sha256,generated,learnable,dialogue_eligible,memory_eligible,memory_exclusion,exclusion,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (sample_id, group_id, role, task, prepared["audio"], prepared["count"] / prepared["rate"], prepared["rate"], prepared["count"], created, checksum, prepared["file_sha256"], int(generated), int(learnable), int(dialogue_eligible), int(not memory_reason), memory_reason, exclusion, encoded_metadata))
            return sample_id
        except BaseException:
            if own_preparation:
                self.discard_audio(prepared)
            raise

    def add_pair(self, group_id, user_sample, assistant_sample, source, quality=1.0, verified=False, created=None, evidence=None):
        quality = float(quality)
        if not math.isfinite(quality) or quality <= 0 or user_sample == assistant_sample:
            raise YuanError("对话配对质量或角色无效 / Invalid dialogue pair quality or roles")
        rows = self.db.execute("SELECT * FROM samples WHERE id IN (?,?)", (user_sample, assistant_sample)).fetchall()
        by_id = {row["id"]: row for row in rows}
        if user_sample not in by_id or assistant_sample not in by_id or by_id[user_sample]["group_id"] != group_id or by_id[assistant_sample]["group_id"] != group_id:
            raise YuanError("对话配对来源不一致 / Dialogue pair provenance mismatch")
        user, assistant = by_id[user_sample], by_id[assistant_sample]
        evidence = dict(evidence or {})
        if verified:
            required = ("reviewer", "annotation_id", "recording_id", "user_speaker", "assistant_speaker", "prompt")
            group = self.db.execute("SELECT source_key FROM groups WHERE id=?", (group_id,)).fetchone()
            valid = (all(isinstance(evidence.get(key), str) and evidence[key].strip() for key in required)
                     and evidence.get("method") == "human_audio_annotation"
                     and evidence.get("language") in ("中文", "English")
                     and evidence.get("roles_verified") is True and evidence.get("response_verified") is True
                     and evidence["user_speaker"] != evidence["assistant_speaker"]
                     and evidence["recording_id"] == group["source_key"]
                     and evidence.get("user_sha256") == user["digest"] and evidence.get("assistant_sha256") == assistant["digest"]
                     and user["role"] == "user" and assistant["role"] == "assistant"
                     and not user["generated"] and not assistant["generated"]
                     and user["dialogue_eligible"] and assistant["dialogue_eligible"])
            if not valid:
                raise YuanError("对话配对缺少可信角色、回复关系或来源证据 / Dialogue pair lacks valid role, reply or provenance evidence")
        pair_id = hashlib.sha256((group_id + "\0" + user_sample + "\0" + assistant_sample + "\0" + str(source)).encode()).hexdigest()
        timestamp = time.time() if created is None else float(created)
        if not math.isfinite(timestamp):
            raise YuanError("对话配对时间无效 / Invalid dialogue pair timestamp")
        with self.transaction():
            self.db.execute("INSERT INTO turn_pairs(id,group_id,user_sample,assistant_sample,source,quality,verified,evidence,created) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(user_sample,assistant_sample) DO UPDATE SET verified=excluded.verified,evidence=excluded.evidence,quality=excluded.quality,eligible=1,cursor=CASE WHEN turn_pairs.evidence<>excluded.evidence THEN 0 ELSE turn_pairs.cursor END,visits=CASE WHEN turn_pairs.evidence<>excluded.evidence THEN 0 ELSE turn_pairs.visits END,retry_at=0,failures=0,error='' WHERE excluded.verified>=turn_pairs.verified",
                            (pair_id, group_id, user_sample, assistant_sample, str(source), quality, int(bool(verified)), json.dumps(evidence, ensure_ascii=False, allow_nan=False, separators=(",", ":")), timestamp))
            self.commit()
        return self.db.execute("SELECT id FROM turn_pairs WHERE user_sample=? AND assistant_sample=?", (user_sample, assistant_sample)).fetchone()[0]

    def assert_independent(self, group_ids):
        if not group_ids:
            return
        placeholders = ",".join("?" for _ in group_ids)
        overlap = self.db.execute("SELECT a.source_key FROM groups a JOIN groups b ON a.source_key=b.source_key AND a.split<>b.split WHERE a.id IN (" + placeholders + ") LIMIT 1", tuple(group_ids)).fetchone()
        repeated = self.db.execute("SELECT a.digest FROM samples a JOIN groups ga ON ga.id=a.group_id JOIN samples b ON b.digest=a.digest AND b.group_id<>a.group_id JOIN groups gb ON gb.id=b.group_id WHERE a.group_id IN (" + placeholders + ") AND ga.split<>gb.split AND (a.learnable=1 OR a.dialogue_eligible=1) AND (b.learnable=1 OR b.dialogue_eligible=1) LIMIT 1", tuple(group_ids)).fetchone()
        if overlap or repeated:
            raise YuanError("训练与留出数据存在同源或重复音频 / Training and held-out data share a source or duplicate audio")

    def db_size(self):
        return sum(path.stat().st_size for path in self.root.glob("memory.sqlite3*") if path.is_file())

    def next_sample(self, split, min_count=2):
        return self.db.execute("SELECT samples.*,groups.split,groups.origin FROM samples JOIN groups ON groups.id=group_id WHERE split=? AND samples.audio_status='ready' AND task='acoustic' AND learnable=1 AND generated=0 AND count>=? ORDER BY visits,samples.created LIMIT 1", (split, max(1, int(min_count)))).fetchone()

    def held_out(self, split, limit, min_count=2):
        return self.db.execute("WITH eligible AS (SELECT s.*,g.split,g.origin,g.source_key,ROW_NUMBER() OVER (PARTITION BY g.source_key ORDER BY s.id) AS position FROM samples s JOIN groups g ON g.id=s.group_id WHERE g.split=? AND s.audio_status='ready' AND s.task='acoustic' AND s.learnable=1 AND s.generated=0 AND s.count>=?) SELECT * FROM eligible WHERE position=1 ORDER BY id LIMIT ?", (split, max(1, int(min_count)), max(1, int(limit)))).fetchall()

    def next_pair(self, split, min_source=2, min_target=2):
        return self.db.execute("SELECT p.*,g.split,g.origin,g.source_key FROM turn_pairs p JOIN groups g ON g.id=p.group_id JOIN samples u ON u.id=p.user_sample JOIN samples a ON a.id=p.assistant_sample WHERE g.split=? AND p.retry_at<=? AND p.eligible=1 AND u.audio_status='ready' AND a.audio_status='ready' AND p.verified=1 AND u.generated=0 AND a.generated=0 AND u.dialogue_eligible=1 AND a.dialogue_eligible=1 AND u.count>=? AND a.count>=? ORDER BY p.visits,p.created LIMIT 1", (split, time.time(), max(1, int(min_source)), max(1, int(min_target)))).fetchone()

    def held_out_pairs(self, split, limit, language=None, min_source=2, min_target=2):
        return self.db.execute("WITH eligible AS (SELECT p.*,g.split,g.origin,g.source_key,ROW_NUMBER() OVER (PARTITION BY g.source_key ORDER BY p.id) AS position FROM turn_pairs p JOIN groups g ON g.id=p.group_id JOIN samples u ON u.id=p.user_sample JOIN samples a ON a.id=p.assistant_sample WHERE g.split=? AND p.retry_at<=? AND p.eligible=1 AND u.audio_status='ready' AND a.audio_status='ready' AND (? IS NULL OR json_extract(p.evidence,'$.language')=?) AND p.verified=1 AND u.generated=0 AND a.generated=0 AND u.dialogue_eligible=1 AND a.dialogue_eligible=1 AND u.count>=? AND a.count>=?) SELECT * FROM eligible WHERE position=1 ORDER BY id LIMIT ?", (split, time.time(), language, language, max(1, int(min_source)), max(1, int(min_target)), max(1, int(limit)))).fetchall()

    def evaluation_pairs(self, split, language, hop):
        return self.db.execute("SELECT p.*,g.split,g.origin,g.source_key FROM turn_pairs p JOIN groups g ON g.id=p.group_id JOIN samples u ON u.id=p.user_sample JOIN samples a ON a.id=p.assistant_sample WHERE g.split=? AND json_extract(p.evidence,'$.language')=? AND p.retry_at<=? AND p.verified=1 AND p.eligible=1 AND u.audio_status='ready' AND a.audio_status='ready' AND u.generated=0 AND a.generated=0 AND u.dialogue_eligible=1 AND a.dialogue_eligible=1 AND u.count>=? AND a.count>=? ORDER BY g.source_key,p.id", (split, language, time.time(), hop, hop)).fetchall()

    def sample(self, sample_id):
        return self.db.execute("SELECT s.*,g.split,g.origin FROM samples s JOIN groups g ON g.id=s.group_id WHERE s.id=?", (sample_id,)).fetchone()

    def read_audio(self, row, start=0, count=None, cancel=None, verify=False):
        import numpy as np
        import soundfile as sf
        def signature(path):
            stat = path.stat()
            return stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_dev, stat.st_ino
        try:
            check(cancel)
            path = safe_path(self.root, row["audio"])
            before = signature(path)
            expected = row["file_sha256"]
            cached = self.integrity_cache.get(row["audio"])
            if verify or cached != (before, expected):
                if not is_digest(expected) or digest_file(path, cancel) != expected:
                    raise InvalidAudio("历史音频内容摘要不匹配 / Historical audio content digest mismatch")
            with sf.SoundFile(str(path)) as stream:
                if stream.samplerate != row["rate"] or stream.channels != 1 or stream.frames != row["count"]:
                    raise InvalidAudio("历史音频格式或长度不匹配 / Historical audio format or length mismatch")
                start = max(0, min(int(start), stream.frames))
                stream.seek(start)
                count = stream.frames - start if count is None else max(0, min(int(count), stream.frames - start))
                values = stream.read(count, dtype="float32", always_2d=False)
            check(cancel)
            if signature(path) != before:
                raise OSError("音频读取期间发生变化 / Audio changed while being read")
            if not values.size or not np.isfinite(values).all():
                raise InvalidAudio("历史音频无效 / Historical audio is invalid")
            limit = self.policy.integer("storage", "integrity_cache_entries", max(16, (os.cpu_count() or 1) * 16), 1, 65536)
            if len(self.integrity_cache) >= limit:
                self.integrity_cache.pop(next(iter(self.integrity_cache)))
            self.integrity_cache[row["audio"]] = (before, expected)
            current = self.db.execute("SELECT audio_status,read_failures FROM samples WHERE id=?", (row["id"],)).fetchone()
            if current and (current["audio_status"] != "ready" or current["read_failures"]):
                self.db.execute("UPDATE samples SET audio_status='ready',retry_at=0,read_failures=0,audio_error='' WHERE id=?", (row["id"],))
                self.commit()
            return values
        except Cancelled:
            raise
        except (OSError, RuntimeError, ValueError) as exc:
            self.audio_failure(row, exc)
            raise

    def advance(self, row, cursor):
        wrapped = cursor >= row["count"]
        self.db.execute("UPDATE samples SET cursor=?,visits=visits+? WHERE id=?", (0 if wrapped else max(0, cursor), int(wrapped), row["id"]))
        self.commit()

    def advance_pair(self, row, cursor, target_count):
        wrapped = cursor >= target_count
        self.db.execute("UPDATE turn_pairs SET cursor=?,visits=visits+? WHERE id=?", (0 if wrapped else max(0, cursor), int(wrapped), row["id"]))
        self.commit()

    def memory_rows(self, exclude_group, model_key, batch_size, cancel=None):
        cursor = self.db.execute("SELECT s.*,m.vector,m.dims FROM samples s JOIN groups g ON g.id=s.group_id JOIN sessions x ON x.id=s.group_id JOIN memories m ON m.sample_id=s.id WHERE g.origin='local' AND s.group_id<>? AND x.ended IS NOT NULL AND s.audio_status='ready' AND s.memory_eligible=1 AND s.count>1 AND m.model_key=? ORDER BY s.created DESC,s.id", (exclude_group, model_key))
        try:
            while True:
                check(cancel)
                rows = cursor.fetchmany(max(1, int(batch_size)))
                if not rows:
                    return
                for row in rows:
                    check(cancel)
                    yield row
        finally:
            cursor.close()

    def memory_count(self, exclude_group):
        return self.db.execute("SELECT count(*) " + self.memory_selection, (exclude_group,)).fetchone()[0]

    def exclude_memory(self, sample_id, reason):
        with self.transaction():
            self.db.execute("UPDATE samples SET memory_eligible=0,memory_exclusion=? WHERE id=?", (str(reason), sample_id))
            self.db.execute("DELETE FROM memories WHERE sample_id=?", (sample_id,))


    def save_memory_vector(self, sample_id, model_key, vector):
        import numpy as np
        values = np.asarray(vector, dtype=np.float32).reshape(-1)
        if not values.size or not np.isfinite(values).all():
            return
        self.db.execute("INSERT OR REPLACE INTO memories(sample_id,model_key,vector,dims,created) SELECT id,?,?,?,? FROM samples WHERE id=? AND memory_eligible=1 AND audio_status='ready'", (model_key, values.tobytes(), len(values), time.time(), sample_id))
        self.commit()

    def stats(self):
        rows = self.db.execute("SELECT origin,count(*) AS n,coalesce(sum(seconds),0) AS seconds,sum(learnable AND audio_status='ready') AS learnable,sum(generated) AS generated,sum(NOT generated AND (NOT learnable OR audio_status<>'ready')) AS excluded FROM samples JOIN groups ON groups.id=group_id GROUP BY origin").fetchall()
        result = {row["origin"]: {"count": row["n"], "seconds": row["seconds"], "learnable": row["learnable"], "generated": row["generated"], "excluded": row["excluded"]} for row in rows}
        pairs = self.db.execute("SELECT g.split,count(*) AS n,sum(p.verified AND p.eligible AND u.dialogue_eligible AND a.dialogue_eligible AND u.audio_status='ready' AND a.audio_status='ready' AND NOT u.generated AND NOT a.generated) AS verified FROM turn_pairs p JOIN groups g ON g.id=p.group_id JOIN samples u ON u.id=p.user_sample JOIN samples a ON a.id=p.assistant_sample GROUP BY g.split").fetchall()
        result["dialogue"] = {"count": sum(row["n"] for row in pairs), "verified": sum(row["verified"] or 0 for row in pairs), **{row["split"]: row["verified"] or 0 for row in pairs}}
        return result

    def close(self):
        try:
            self.db.execute("PRAGMA wal_checkpoint(PASSIVE)")
        finally:
            self.db.close()

    def commit(self):
        if not self.transaction_depth:
            try:
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

    @contextmanager
    def transaction(self):
        outer = self.transaction_depth == 0
        if outer:
            self.db.execute("BEGIN IMMEDIATE")
        self.transaction_depth += 1
        try:
            yield
            if outer:
                self.db.commit()
        except BaseException:
            if outer:
                self.db.rollback()
            raise
        finally:
            self.transaction_depth -= 1

    def retry_delay(self, attempts, invalid=False):
        base = self.policy.number("storage", "audio_retry_seconds", max(0.1, math.sqrt(tick())), tick(), 3600.0)
        ceiling = self.policy.number("storage", "audio_retry_max_seconds", max(60.0, base), base, 86400.0)
        if invalid:
            base = self.policy.number("storage", "integrity_recheck_seconds", ceiling, base, 86400.0)
        return min(max(base, ceiling), base * min(max(1, attempts), math.sqrt(max(base, ceiling) / base)) ** 2)

    def audio_failure(self, row, exc):
        if row is None or getattr(exc, "yuan_sample", None) == row["id"]:
            return
        current = self.db.execute("SELECT read_failures,audio FROM samples WHERE id=?", (row["id"],)).fetchone()
        if current is None:
            return
        invalid = isinstance(exc, InvalidAudio)
        attempts = current["read_failures"] + 1
        self.db.execute("UPDATE samples SET audio_status=?,retry_at=?,read_failures=?,audio_error=? WHERE id=?",
                        ("invalid" if invalid else "retry", time.time() + self.retry_delay(attempts, invalid), attempts, concise_error(exc), row["id"]))
        self.db.execute("DELETE FROM memories WHERE sample_id=?", (row["id"],))
        self.integrity_cache.pop(current["audio"], None)
        self.commit()
        try:
            exc.yuan_sample = row["id"]
        except (AttributeError, TypeError):
            pass

    def pair_failure(self, row, exc):
        current = self.db.execute("SELECT failures FROM turn_pairs WHERE id=?", (row["id"],)).fetchone()
        if current is None:
            return
        attempts = current[0] + 1
        self.db.execute("UPDATE turn_pairs SET retry_at=?,failures=?,error=? WHERE id=?",
                        (time.time() + self.retry_delay(attempts), attempts, concise_error(exc), row["id"]))
        self.commit()

    def recheck_audio(self, cancel=None):
        limit = self.policy.integer("storage", "recheck_batch", max(1, math.isqrt(os.cpu_count() or 1)), 1, max(1, os.cpu_count() or 1))
        rows = self.db.execute("SELECT * FROM samples WHERE audio_status<>'ready' AND retry_at<=? ORDER BY retry_at,id LIMIT ?", (time.time(), limit)).fetchall()
        recovered = 0
        for row in rows:
            check(cancel)
            try:
                self.read_audio(row, 0, min(row["count"], row["rate"]), cancel=cancel, verify=True)
                recovered += 1
            except Cancelled:
                raise
            except (OSError, RuntimeError, ValueError):
                pass
        return recovered

    def prepare_audio(self, waveform, rate, cancel=None, purpose="learning"):
        import numpy as np
        import soundfile as sf
        waveform = np.asarray(waveform, dtype=np.float32).reshape(-1)
        if not waveform.size or not np.isfinite(waveform).all() or isinstance(rate, bool) or not isinstance(rate, int) or rate <= 0:
            raise InvalidAudio("音频样本无效 / Invalid audio sample")
        if np.max(np.abs(waveform)) > 1:
            raise InvalidAudio("音频超出可保存范围 / Audio is outside the supported range")
        canonical = np.floor(waveform.astype(np.float64) * (1 << 23)).clip(-(1 << 23), (1 << 23) - 1).astype("<i4")
        checksum = hashlib.sha256(canonical.tobytes() + str(rate).encode("ascii")).hexdigest()
        target = safe_path(self.audio, checksum + ".flac")
        temporary = self.audio / (checksum + "-" + uuid.uuid4().hex + ".partial")
        new_file = not target.is_file()
        with StorageBudget(self.root.parent).reserve(waveform.nbytes * 2 + self.db_size(), purpose):
            try:
                check(cancel)
                sf.write(str(temporary), canonical.astype(np.float32) / (1 << 23), rate, format="FLAC", subtype="PCM_24")
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                file_sha256 = digest_file(temporary, cancel)
                check(cancel)
                existing_hash = None if new_file else digest_file(target, cancel)
                known = self.db.execute("SELECT file_sha256 FROM samples WHERE audio=? LIMIT 1", (target.relative_to(self.root).as_posix(),)).fetchone()
                if known and existing_hash == known["file_sha256"]:
                    file_sha256 = existing_hash
                elif new_file or existing_hash != file_sha256:
                    os.replace(temporary, target)
                    sync_dir(target.parent)
                    self.integrity_cache.pop(target.relative_to(self.root).as_posix(), None)
                    if known:
                        with self.transaction():
                            self.db.execute("UPDATE samples SET file_sha256=?,audio_status='ready',retry_at=0,read_failures=0,audio_error='' WHERE audio=? AND digest=?",
                                            (file_sha256, target.relative_to(self.root).as_posix(), checksum))
                return {"audio": target.relative_to(self.root).as_posix(), "digest": checksum, "file_sha256": file_sha256,
                        "rate": rate, "count": len(waveform), "new_file": new_file}
            finally:
                temporary.unlink(missing_ok=True)

    def discard_audio(self, prepared):
        if prepared.get("new_file") and not self.db.execute("SELECT 1 FROM samples WHERE audio=?", (prepared["audio"],)).fetchone():
            safe_path(self.root, prepared["audio"]).unlink(missing_ok=True)
            self.integrity_cache.pop(prepared["audio"], None)

    @contextmanager
    def audio_files(self, cancel=None):
        while not self.file_lock.acquire(timeout=math.sqrt(tick())):
            check(cancel)
        try:
            check(cancel)
            yield
        finally:
            self.file_lock.release()


    def exclude_task(self, row, task, reason):
        if task == "dialogue":
            self.db.execute("UPDATE turn_pairs SET eligible=0,eligibility_error=?,retry_at=0 WHERE id=?", (str(reason), row["id"]))
        elif task == "acoustic":
            self.db.execute("UPDATE samples SET learnable=0,exclusion=? WHERE id=?", ("acoustic: " + str(reason), row["id"]))
        else:
            raise ValueError(task)
        self.commit()


def owned_release_preflight(state, emit):
    state = Path(state)
    paths = (state.parent / "Yuan.release.json", Path(__file__).resolve().with_name("Yuan.release.json"))
    descriptor = next((path for path in paths if path.is_file() and not path.is_symlink()), None)
    previous = read_json(state / "owned-release.json")
    try:
        if descriptor:
            value = checked_json(descriptor)
            source = value.get("source")
            if value.get("format") != "yuan-owned-release-source-v2" or value.get("owner") != APP_NAME or not is_digest(value.get("sha256")) or not isinstance(source, str) or not source.strip():
                raise YuanError("自有模型来源配置无效 / Invalid owned model source configuration")
            installed = safe_path(state, "owned-releases/" + value["sha256"])
            records = (previous, read_json(state / "owned-release-pending.json"))
            reusable = any(record.get("bundle_sha256") == value["sha256"] and is_digest(record.get("manifest_sha256")) for record in records)
            reusable = reusable and installed.is_dir() and safe_path(installed, "release-manifest.json").is_file()
            parsed = urllib.parse.urlparse(source)
            if parsed.scheme:
                if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
                    raise YuanError("自有模型只接受受控 HTTPS 来源 / Owned models require a controlled HTTPS source")
            else:
                archive = safe_path(descriptor.parent, source)
                if archive.is_symlink() or not archive.is_file() and not reusable:
                    raise YuanError("自有模型交付包不存在 / Owned model delivery package is missing")
            build = value.get("build_sha256")
            if build is not None and build != digest_file(Path(__file__)):
                raise YuanError("代码与自有模型交付版本不一致，请由发布端重新验收并导出配套包 / Code and owned model delivery differ; the publisher must revalidate and export a matching bundle")
            ReleaseTrust(state).keys()
            emit("deployment_preflight", key="owned_source_found", source_configured=True, accepted=False, descriptor=str(descriptor))
            return True
        if previous.get("directory") and is_digest(previous.get("manifest_sha256")):
            root = safe_path(state, previous["directory"])
            if not root.is_dir() or not safe_path(root, "release-manifest.json").is_file():
                raise YuanError("已安装的自有模型清单缺失 / Installed owned model manifest is missing")
            ReleaseTrust(state).keys()
            emit("deployment_preflight", key="owned_source_found", source_configured=True, accepted=False)
            return True
        emit("deployment_preflight", key="native_bootstrap", source_configured=False, accepted=False, automatic=True)
        return False
    except YuanFault:
        raise
    except (OSError, ValueError, TypeError, YuanError) as exc:
        emit("deployment_preflight", key="owned_source_invalid", source_configured=False, accepted=False, reason=concise_error(exc))
        raise YuanFault(concise_error(exc), category="configuration", code="owned_source_invalid", stage="stage_owned_release") from exc

class ReleaseTrust:
    def __init__(self, state):
        self.state = Path(state).resolve()

    @staticmethod
    def canonical(payload):
        return json.dumps({key: value for key, value in payload.items() if key != "signature"}, ensure_ascii=False,
                          sort_keys=True, allow_nan=False, separators=(",", ":")).encode("utf-8")

    def keys(self):
        locations = (Path(__file__).resolve().with_name("Yuan.trust.json"), self.state.parent / "Yuan.trust.json")
        path = next((path for path in locations if path.is_file() and not path.is_symlink()), None)
        if path is None:
            raise YuanError("缺少独立交付的发布信任密钥 / Independently supplied publisher trust key is missing")
        data = checked_json(path)
        keys = data.get("keys")
        if data.get("format") != "yuan-release-trust-v1" or not isinstance(keys, dict) or not keys:
            raise YuanError("发布信任配置无效 / Invalid publisher trust configuration")
        for identifier, value in keys.items():
            if not is_digest(value) or hashlib.sha256(bytes.fromhex(value)).hexdigest() != identifier:
                raise YuanError("发布信任密钥标识不匹配 / Publisher trust key identity mismatch")
        return keys

    def verify(self, payload):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        from cryptography.exceptions import InvalidSignature
        signature = payload.get("signature", {})
        if not isinstance(signature, dict) or signature.get("algorithm") != "Ed25519":
            raise YuanError("自有模型交付缺少可信签名 / Owned model delivery lacks a trusted signature")
        keys = self.keys()
        public = keys.get(signature.get("key_id"))
        value = signature.get("value")
        if public is None or not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{128}", value) is None:
            raise YuanError("签名密钥不受信任或签名格式无效 / Untrusted signing key or invalid signature encoding")
        try:
            Ed25519PublicKey.from_public_bytes(bytes.fromhex(public)).verify(bytes.fromhex(value), self.canonical(payload))
        except (ValueError, TypeError, InvalidSignature) as exc:
            raise YuanError("自有模型签名校验失败 / Owned model signature validation failed") from exc
        return signature["key_id"]

    def signing_key(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        path = safe_path(self.state, "publisher/release-signing.pem")
        if not path.is_file() or path.is_symlink():
            raise YuanError("开发端发布签名密钥缺失 / Publisher signing key is missing")
        if os.name != "nt" and path.stat().st_mode & 0o077:
            raise YuanError("发布私钥访问权限过宽 / Publisher private-key permissions are too broad")
        key = serialization.load_pem_private_key(path.read_bytes(), password=None)
        if not isinstance(key, Ed25519PrivateKey):
            raise YuanError("发布签名密钥类型无效 / Invalid publisher signing-key type")
        return key

    def sign(self, payload):
        from cryptography.hazmat.primitives import serialization
        key = self.signing_key()
        public = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        identifier = hashlib.sha256(public).hexdigest()
        if self.keys().get(identifier) != public.hex():
            raise YuanError("签名私钥与独立信任配置不匹配 / Signing key does not match the independent trust configuration")
        result = {name: value for name, value in payload.items() if name != "signature"}
        result["signature"] = {"algorithm": "Ed25519", "key_id": identifier, "value": key.sign(self.canonical(result)).hex()}
        self.verify(result)
        return result

    def initialize_publisher(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        path = safe_path(self.state, "publisher/release-signing.pem")
        trust = self.state.parent / "Yuan.trust.json"
        if path.exists() or trust.exists():
            raise YuanError("已有发布身份不会被覆盖 / Existing publisher identity will not be overwritten")
        path.parent.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            path.parent.chmod(0o700)
        key = Ed25519PrivateKey.generate()
        public = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
            stream.flush()
            os.fsync(stream.fileno())
        atomic_json(trust, {"format": "yuan-release-trust-v1", "keys": {hashlib.sha256(public).hexdigest(): public.hex()}})
        return trust

class OwnedReleaseInstaller:
    def __init__(self, state, emit):
        self.state, self.emit = Path(state), emit
        self.policy = AdaptivePolicy(state)
        self.pointer = safe_path(state, "owned-release.json")

    def _manifest(self, root, cancel, expected=None):
        path = safe_path(root, "release-manifest.json")
        if expected is not None and digest_file(path, cancel) != expected:
            raise YuanError("自有模型清单摘要不匹配 / Owned model manifest digest mismatch")
        manifest = checked_json(path)
        ReleaseTrust(self.state).verify(manifest)
        provenance = manifest.get("provenance", {})
        if (manifest.get("format") != "yuan-owned-release-v3" or provenance.get("owner") != APP_NAME
                or provenance.get("self_trained") is not True or provenance.get("external_model") is not False
                or provenance.get("text_to_speech") is not False or manifest.get("evaluator_sha256") != behavior_digest()):
            raise YuanError("自有模型来源声明或验收程序不匹配 / Owned model provenance declaration or evaluator mismatch")
        if manifest.get("build_sha256") != digest_file(Path(__file__), cancel):
            raise YuanError("代码与自有模型交付版本不一致，请由发布端重新验收并导出配套包 / Code and owned model delivery differ; the publisher must revalidate and export a matching bundle")
        files = manifest.get("files")
        if not isinstance(files, dict) or not files or not all(isinstance(name, str) and is_digest(checksum) for name, checksum in files.items()):
            raise YuanError("自有模型文件清单无效 / Invalid owned model file manifest")
        if not {"learning/model.json", "learning/release.json", "learning/training-provenance.json", "learning/acceptance-policy.json"}.issubset(files):
            raise YuanError("自有模型缺少结构或验收记录 / Owned model lacks configuration or acceptance records")
        for name, checksum in files.items():
            check(cancel)
            path = safe_path(root, name)
            if path.is_symlink() or not path.is_file() or digest_file(path, cancel) != checksum:
                raise YuanError("自有模型文件校验失败 / Owned model file validation failed: " + name)
        provenance_record = checked_json(safe_path(root, "learning/training-provenance.json"))
        if (provenance_record.get("format") != "yuan-training-provenance-v1" or provenance_record.get("identity") != manifest.get("identity")
                or provenance_record.get("owner") != APP_NAME or provenance_record.get("external_model") is not False
                or not isinstance(provenance_record.get("steps"), int) or provenance_record["steps"] <= 0
                or not is_digest(provenance_record.get("lineage_sha256"))):
            raise YuanError("自有模型训练来源记录不完整 / Owned model training provenance is incomplete")
        review = checked_json(safe_path(root, "learning/release.json"))
        evaluation_name = review.get("evaluation_file")
        if not isinstance(evaluation_name, str) or "learning/" + evaluation_name not in files:
            raise YuanError("验收结果不在签名清单中 / Acceptance result is absent from the signed manifest")
        evaluation = checked_json(safe_path(root, "learning/" + evaluation_name))
        model = evaluation.get("model", {})
        if provenance_record.get("model_sha256") != model.get("sha256") or provenance_record["steps"] != model.get("steps") or "learning/" + str(model.get("file")) not in files:
            raise YuanError("训练来源记录与验收模型不一致 / Training provenance does not match the accepted model")
        for case in evaluation.get("cases", []):
            if any("learning/" + str(case.get(field + "_file")) not in files for field in ("input", "output")):
                raise YuanError("听测音频不在签名清单中 / Listening audio is absent from the signed manifest")
        return manifest

    def _extract(self, archive, root, cancel):
        import psutil
        free = StorageBudget(self.state).snapshot()["available_bytes"]
        budget = self.policy.integer("deployment", "bundle_unpacked_bytes", free // 3, 1, max(1, free))
        entries = self.policy.integer("deployment", "bundle_file_limit", 4096, 1, 1048576)
        manifest_limit = self.policy.integer("deployment", "manifest_bytes", min(4 * 1024 * 1024, psutil.virtual_memory().available // 16), 1, max(1, psutil.virtual_memory().available // 8))
        with zipfile.ZipFile(archive) as bundle:
            items = bundle.infolist()
            if len(items) > entries or sum(item.file_size for item in items) > budget:
                raise YuanError("自有模型包超过解包预算 / Owned model package exceeds extraction budget")
            names = set()
            file_items = {}
            for item in items:
                check(cancel)
                name = item.filename.rstrip("/")
                path = safe_path(root, name)
                if name != path.relative_to(root).as_posix() or stat.S_ISLNK(item.external_attr >> 16) or name.casefold() in names or item.flag_bits & 1:
                    raise YuanError("模型包包含不安全路径、链接或重复项 / Model package contains an unsafe path, link or duplicate")
                names.add(name.casefold())
                if item.is_dir():
                    if name.split("/")[0] not in ("learning", "datasets"):
                        raise YuanError("模型包目录无效 / Invalid model package directory")
                    continue
                if name != "release-manifest.json" and (name.split("/")[0] not in ("learning", "datasets") or path.suffix.lower() not in (".json", ".pt", ".flac", ".wav", ".ogg", ".opus", ".mp3", ".aif", ".aiff")):
                    raise YuanError("模型包包含非运行资产 / Model package contains an unsupported asset")
                file_items[name] = item
            info = file_items.get("release-manifest.json")
            if info is None or info.file_size > manifest_limit:
                raise YuanError("自有模型清单缺失或过大 / Owned model manifest is missing or too large")
            manifest = json.loads(bundle.read(info))
            if not isinstance(manifest, dict):
                raise YuanError("模型包清单无效 / Invalid model package manifest")
            ReleaseTrust(self.state).verify(manifest)
            files = manifest.get("files", {})
            if not isinstance(files, dict) or set(files) != set(file_items) - {"release-manifest.json"}:
                raise YuanError("模型包文件与清单不一致 / Model package files do not match its manifest")
            for name, item in file_items.items():
                check(cancel)
                path = safe_path(root, name)
                path.parent.mkdir(parents=True, exist_ok=True)
                written = 0
                checksum = hashlib.sha256()
                with bundle.open(item) as incoming, path.open("xb") as outgoing:
                    while True:
                        check(cancel)
                        block = incoming.read(min(1024 * 1024, budget + 1))
                        if not block:
                            break
                        written += len(block)
                        if written > item.file_size or written > budget:
                            raise YuanError("模型包解压大小不匹配 / Model package extraction size mismatch")
                        checksum.update(block)
                        outgoing.write(block)
                    outgoing.flush()
                    os.fsync(outgoing.fileno())
                if written != item.file_size or name in files and checksum.hexdigest() != files[name]:
                    raise YuanError("模型包资产摘要不匹配 / Model package asset digest mismatch")
        return self._manifest(root, cancel)

    def _import(self, root, cancel):
        import numpy as np
        configuration = checked_json(safe_path(root, "learning/model.json"))
        limits = {"rate": 96000, "hop": 96000, "width": 512, "layers": 8}
        for key, default in limits.items():
            value = configuration.get(key)
            maximum = self.policy.integer("deployment", key + "_limit", default, 1, max(default, default * 4))
            if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= maximum:
                raise YuanError("自有模型结构超过执行边界 / Owned model configuration exceeds execution bounds")
        if configuration["hop"] > configuration["rate"]:
            raise YuanError("自有模型帧长度无效 / Invalid owned model frame length")
        normalizer = types.SimpleNamespace(np=np, rate=configuration["rate"], layers=configuration["layers"])
        normalizer.normalize = lambda audio, rate: NativeAudio.normalize(normalizer, audio, rate)
        store = AudioStore(root, recover=False)
        try:
            importer = DatasetImporter(root, store, normalizer, self.emit, namespace="owned")
            while importer.step(cancel):
                check(cancel)
            manifests = list(safe_path(root, "datasets").glob("*.json"))
            if not manifests or any(store.setting(importer.identity_key(path, checked_json(path))) != digest_file(path, cancel) for path in manifests):
                raise YuanError("自有模型听测数据未完整导入 / Owned model listening data was not completely imported")
        finally:
            store.close()

    def prepare(self, cancel):
        self.state.mkdir(parents=True, exist_ok=True)
        candidates = (self.state.parent / "Yuan.release.json", Path(__file__).resolve().with_name("Yuan.release.json"))
        descriptor_path = next((path for path in candidates if path.is_file() and not path.is_symlink()), None)
        previous = read_json(self.pointer)
        if descriptor_path is None:
            if previous.get("directory") and is_digest(previous.get("manifest_sha256")):
                root = safe_path(self.state, previous["directory"])
                self._manifest(root, cancel, previous["manifest_sha256"])
                self._import(root, cancel)
                return root
            self.emit("deployment", key="native_bootstrap", source_configured=False, model_supplied=False, automatic=True)
            return None
        descriptor = checked_json(descriptor_path)
        if (descriptor.get("format") != "yuan-owned-release-source-v2" or descriptor.get("owner") != APP_NAME
                or not is_digest(descriptor.get("sha256")) or not isinstance(descriptor.get("source"), str)):
            raise YuanError("自有模型交付来源配置无效 / Invalid owned model delivery source configuration")
        ReleaseTrust(self.state).verify(descriptor)
        checksum = descriptor["sha256"]
        root = safe_path(self.state, "owned-releases/" + checksum)
        pending = read_json(safe_path(self.state, "owned-release-pending.json"), {})
        if pending.get("bundle_sha256") == checksum:
            previous = pending
        if root.is_dir() and previous.get("bundle_sha256") == checksum and is_digest(previous.get("manifest_sha256")):
            self._manifest(root, cancel, previous["manifest_sha256"])
            self._import(root, cancel)
            return root
        self.emit("deployment", key="owned_release_installing", source_configured=True, bundle_sha256=checksum)
        archive = safe_path(self.state, "owned-release-downloads/" + checksum + ".zip")
        archive.parent.mkdir(parents=True, exist_ok=True)
        source = descriptor["source"]
        if urllib.parse.urlparse(source).scheme:
            parsed = urllib.parse.urlparse(source)
            if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
                raise YuanError("自有模型只接受受控 HTTPS 来源 / Owned models require a controlled HTTPS source")
            Network(cancel, self.policy).download(source, archive, expected_hash=checksum, max_bytes=max(1, StorageBudget(self.state).snapshot()["available_bytes"] // 3),
                                                progress=lambda done, total: self.emit("source_progress", component="owned_release", current=done, total=total))
        else:
            source_path = safe_path(descriptor_path.parent, source)
            if source_path.is_symlink() or digest_file(source_path, cancel) != checksum:
                raise YuanError("自有模型交付包摘要不匹配 / Owned model delivery package digest mismatch")
            archive = source_path
        if digest_file(archive, cancel) != checksum:
            raise YuanError("自有模型交付包校验失败 / Owned model delivery package verification failed")
        root.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(dir=root.parent, prefix=".install-"))
        try:
            self._extract(archive, staging, cancel)
            self._import(staging, cancel)
            check(cancel)
            if root.exists():
                shutil.rmtree(root)
            os.replace(staging, root)
            sync_dir(root.parent)
            manifest_sha256 = digest_file(root / "release-manifest.json", cancel)
            atomic_json(safe_path(self.state, "owned-release-pending.json"), {"directory": root.relative_to(self.state).as_posix(), "bundle_sha256": checksum,
                                       "manifest_sha256": manifest_sha256, "source_config_sha256": digest_file(descriptor_path, cancel)}, cancel)
            self.emit("deployment", key="owned_release_verifying", model_supplied=True, bundle_sha256=checksum)
            return root
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

    def activate(self, root, model, cancel):
        root = Path(root).resolve()
        if not model.playback_ready or not model.dialogue_ready or Path(model.release_root).resolve() != root:
            raise YuanError("未通过验收的交付不能激活 / An unaccepted delivery cannot be activated")
        pending_path = safe_path(self.state, "owned-release-pending.json")
        pending = read_json(pending_path, {})
        if pending.get("directory") != root.relative_to(self.state).as_posix():
            previous = read_json(self.pointer, {})
            if previous.get("directory") == root.relative_to(self.state).as_posix():
                return
            raise YuanError("交付激活指针不匹配 / Delivery activation pointer mismatch")
        self._manifest(root, cancel, pending.get("manifest_sha256"))
        previous = read_json(self.pointer, {})
        if previous.get("directory") and previous.get("directory") != pending["directory"]:
            pending["previous"] = {key: previous[key] for key in ("directory", "manifest_sha256", "bundle_sha256") if key in previous}
        pending["serving_model"] = model.serving_digest
        local_review = safe_path(self.state, "learning/release.json")
        if local_review.is_file():
            pending["superseded_local_review_sha256"] = digest_file(local_review, cancel)
        pending["activated_at"] = time.time()
        atomic_json(self.pointer, pending, cancel)
        try:
            pending_path.unlink(missing_ok=True)
            archive = safe_path(self.state, "owned-release-downloads/" + pending["bundle_sha256"] + ".zip")
            archive.unlink(missing_ok=True)
        except OSError as exc:
            self.emit("notice", key="storage_cleanup", reason=concise_error(exc), serving_preserved=True)
        self.emit("deployment", key="release_loaded", model_supplied=True, conversation_verified=True, serving_digest=model.serving_digest)

    def export(self, model, cancel):
        if not model.refresh_release() or not model.playback_ready:
            raise YuanError("只有已验收的自有模型可以交付 / Only an accepted owned model can be delivered")
        ReleaseTrust(self.state).signing_key()
        root = getattr(model, "release_root", model.state)
        learning = safe_path(root, "learning")
        review = checked_json(learning / "release.json")
        evaluation = checked_json(safe_path(learning, review["evaluation_file"]))
        training = evaluation["model"].get("training_provenance", {})
        if (training.get("owner") != APP_NAME or training.get("self_trained") is not True or training.get("external_model") is not False
                or not is_digest(training.get("lineage_sha256")) or evaluation["model"].get("steps", 0) <= 0):
            raise YuanError("缺少可绑定到当前模型的自有训练记录 / Owned training records bound to this model are missing")
        provenance = {"format": "yuan-training-provenance-v1", "identity": model.identity(), "model_sha256": model.serving_digest,
                      "steps": evaluation["model"]["steps"], **training}
        atomic_json(learning / "training-provenance.json", provenance, cancel)
        files = {"learning/model.json", "learning/release.json", "learning/training-provenance.json", "learning/acceptance-policy.json",
                 "learning/" + review["evaluation_file"], "learning/" + evaluation["model"]["file"]}
        recordings = {case["recording_id"] for case in evaluation["cases"]}
        for case in evaluation["cases"]:
            files.update("learning/" + case[field + "_file"] for field in ("input", "output"))
        sources = {name: safe_path(root, name) for name in files}
        found = set()
        splits = set()
        dataset_roots = {Path(value).resolve() for value in (root, model.state, model.owned_root) if value is not None}
        for dataset_root in sorted(dataset_roots):
            datasets = safe_path(dataset_root, "datasets")
            for path in sorted(datasets.glob("*.json")):
                data = checked_json(path)
                splits.add(data.get("split"))
                if data.get("recording_id") in recordings:
                    if data.get("split") != "release":
                        raise YuanError("交付听测数据必须显式划分为发布留出集 / Delivery listening datasets must explicitly use the release split")
                    found.add(data["recording_id"])
                assets = {"datasets/" + path.name: path}
                for turn in data["turns"]:
                    asset = safe_path(datasets, turn["audio"])
                    assets["datasets/" + asset.relative_to(datasets).as_posix()] = asset
                for name, asset in assets.items():
                    if name in sources and digest_file(sources[name], cancel) != digest_file(asset, cancel):
                        raise YuanError("不同来源的数据集文件名发生冲突 / Dataset filenames conflict across sources")
                    sources[name] = asset
        if found != recordings:
            raise YuanError("交付缺少可重建的原始听测数据 / Delivery lacks reconstructable original listening data")
        files = set(sources)
        mapping = {name: digest_file(sources[name], cancel) for name in sorted(files)}
        manifest = {"format": "yuan-owned-release-v3", "provenance": {"owner": APP_NAME, "self_trained": True, "external_model": False, "text_to_speech": False},
                    "evaluator_sha256": behavior_digest(), "build_sha256": digest_file(Path(__file__), cancel), "identity": model.identity(), "files": mapping}
        if not {"train", "validation", "guard", "release"}.issubset(splits):
            raise YuanError("交付包需要训练、验证、保护与发布留出数据 / Delivery requires training, validation, guard and release-held-out data")
        manifest = ReleaseTrust(self.state).sign(manifest)
        directory = safe_path(self.state, "exports/" + model.serving_digest)
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / "Yuan.release.zip"
        temporary = target.with_suffix(".partial")
        try:
            with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("release-manifest.json", json.dumps(manifest, ensure_ascii=False, allow_nan=False, separators=(",", ":")))
                for name in sorted(files):
                    check(cancel)
                    path = sources[name]
                    archive.write(path, name)
                    if digest_file(path, cancel) != mapping[name]:
                        raise YuanError("交付期间资产发生变化 / An asset changed during delivery export")
            with temporary.open("rb") as stream:
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            descriptor = ReleaseTrust(self.state).sign({"format": "yuan-owned-release-source-v2", "owner": APP_NAME,
                                                       "source": target.name, "sha256": digest_file(target, cancel),
                                                       "build_sha256": digest_file(Path(__file__), cancel)})
            atomic_json(directory / "Yuan.release.json", descriptor, cancel)
            atomic_json(directory / "Yuan.trust.json", {"format": "yuan-release-trust-v1", "keys": ReleaseTrust(self.state).keys()}, cancel)
            shutil.copyfile(Path(__file__), directory / "Yuan.py")
        finally:
            temporary.unlink(missing_ok=True)
        self.emit("notice", key="owned_release_exported", directory=str(directory), model=model.serving_digest, reason=str(directory))
        return target

class AudioSequence:
    def __init__(self, count, reader):
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0 or not callable(reader):
            raise ValueError("音频序列无效 / Invalid audio sequence")
        self.count, self.reader = count, reader

    def __len__(self):
        return self.count

    def blocks(self, size, cancel=None):
        import numpy as np
        size = max(1, int(size))
        for start in range(0, self.count, size):
            check(cancel)
            count = min(size, self.count - start)
            values = np.asarray(self.reader(start, count), dtype=np.float32)
            if values.ndim != 1 or len(values) != count or not np.isfinite(values).all():
                raise InvalidAudio("音频序列缺失或损坏 / Audio sequence is incomplete or invalid")
            yield values


class AudioEncoding:
    def __init__(self, hidden, pending, condition):
        self.hidden, self.pending, self.condition = hidden, pending, condition


RUNTIME_MODULES = ("numpy", "torch", "psutil", "certifi", "sounddevice", "soundfile", "soxr", "cryptography")



def import_runtime_module(name, state, emit, cancel, supervisor, budgets):
    import importlib
    check(cancel)
    key = "stage_import_" + name
    budget = supervisor.budget(key, budgets.get(key))
    stall = supervisor.policy.number("supervision", key + "_stall_seconds", budget, supervisor.minimum, budget)
    with operation(emit, key, "import", budget_seconds=budget, stall_seconds=stall, component=name):
        try:
            module = importlib.import_module(name)
            check(cancel)
            emit("dependency_loaded", key="dependency_loaded", component=name, version=str(getattr(module, "__version__", "")))
            return module
        except Cancelled:
            raise
        except BaseException as exc:
            raise YuanFault("运行组件导入失败 / Runtime component import failed", category="integrity" if isinstance(exc, ModuleNotFoundError) else failure_category(exc),
                            code="dependency_import_failed", stage=key, component=name) from exc

def load_runtime_modules(state, emit, cancel=None):
    state = Path(state).resolve()
    identity = os.environ.get("YUAN_IMPORT_IDENTITY")
    supervisor = StageSupervisor(state, emit, identity=identity if is_digest(identity) else None)
    try:
        budgets = json.loads(os.environ.get("YUAN_IMPORT_BUDGETS", "{}"))
    except (ValueError, TypeError):
        budgets = {}
    if not isinstance(budgets, dict):
        budgets = {}
    emit("runtime_profile", python=platform.python_version(), executable=sys.executable, system=platform.system(), machine=platform.machine(),
         cwd=str(Path.cwd()), environment=supervisor.fingerprint, mode="probe" if "--probe" in sys.argv else "engine")
    modules = {name: import_runtime_module(name, state, emit, cancel, supervisor, budgets) for name in RUNTIME_MODULES}
    supervisor.policy.save()
    emit("runtime_profile", components={name: str(getattr(module, "__version__", "")) for name, module in modules.items()})
    return modules

class AcceptancePolicy:
    dimensions = ("prompt", "input", "history")
    languages = ("中文", "English")

    def __init__(self, model):
        root = model.owned_root if model.owned_root is not None else model.state
        self.path = safe_path(root, "learning/acceptance-policy.json")
        if not self.path.exists() and model.owned_root is None:
            source = safe_path(model.state.parent, "Yuan.acceptance.json")
            if os.environ.get("YUAN_PUBLISHER") == "1" and source.is_file():
                data = checked_json(source)
            else:
                data = {"format": "yuan-acceptance-policy-v1", "scenarios": list(self.dimensions),
                        "confidence": model.policy.number("acceptance", "confidence", 0.95, 0.5, 0.999),
                        "maximum_failure_rate": model.policy.number("acceptance", "maximum_failure_rate", 0.05, 0.001, 0.1)}
            self.validate(data)
            atomic_json(self.path, data, model.cancel)
        self.data = checked_json(self.path)
        self.validate(self.data)
        self.confidence = float(self.data["confidence"])
        self.maximum_failure = float(self.data["maximum_failure_rate"])
        self.minimum = max(2, math.ceil(math.log1p(-self.confidence) / math.log1p(-self.maximum_failure)))
        self.digest = payload_digest(self.data)

    @classmethod
    def validate(cls, data):
        if data.get("format") != "yuan-acceptance-policy-v1":
            raise YuanError("缺少开发端验收策略 / Publisher acceptance policy is missing")
        for key in ("confidence", "maximum_failure_rate"):
            value = data.get(key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value < 1:
                raise YuanError("验收置信度或风险边界无效 / Invalid acceptance confidence or risk bound")
        scenarios = data.get("scenarios")
        if not isinstance(scenarios, list) or set(scenarios) != set(cls.dimensions) or len(scenarios) != len(cls.dimensions):
            raise YuanError("验收必须覆盖提示词、输入与多轮历史 / Acceptance must cover prompt, input and multi-turn history")

    def select(self, store, model, split="release", require_controls=False):
        selected = {}
        for language in self.languages:
            rows = store.evaluation_pairs(split, language, model.hop)
            for dimension in self.dimensions:
                sources = set()
                for row in rows:
                    evidence = json.loads(row["evidence"])
                    if (dimension not in evidence.get("scenarios", []) or row["source_key"] in sources
                            or require_controls and dimension not in evidence.get("contrasts", {})):
                        continue
                    selected[row["id"]] = row
                    sources.add(row["source_key"])
                    if len(sources) >= self.minimum:
                        break
                if len(sources) < self.minimum:
                    return []
        return list(selected.values())

    def check_coverage(self, cases):
        for language in self.languages:
            for dimension in self.dimensions:
                sources = {case["recording_id"] for case in cases if case.get("language") == language and dimension in case.get("scenarios", [])}
                if len(sources) < self.minimum:
                    raise YuanError("独立中英文场景覆盖未达到验收置信要求 / Independent Chinese and English scenario coverage does not meet the acceptance confidence requirement")

class NativeAudio:
    def __init__(self, state, cancel, emit, owned_root=None, dependencies=None):
        dependencies = dependencies if dependencies is not None else load_runtime_modules(state, emit, cancel)
        np, psutil, torch = (dependencies[name] for name in ("numpy", "psutil", "torch"))
        self.np, self.torch, self.emit, self.state = np, torch, emit, Path(state)
        self.cancel = cancel
        self.owned_root = Path(owned_root) if owned_root is not None else None
        self.release_root = self.state
        self.input_budget = None
        self.steps = 0
        self.accepted = 0
        self.acoustic_ready = False
        self.dialogue_ready = False
        self.dialogue_trained = False
        self.playback_ready = False
        self.serving_network = None
        self.serving_digest = None
        self.release_signature = None
        self.release_failure = None
        self.capacity_scale = 1.0
        self.training_retry_at = 0.0
        self.training_oom_window = None
        self.active_digest = None
        self.policy = AdaptivePolicy(self.state)
        with operation(emit, "stage_device"):
            self.device = self._device()
        self.dtype = torch.float32
        with operation(emit, "stage_configuration"):
            self.rate, self.hop, self.width, self.layers, self.chunk_seconds, self.capacity_seconds = self._configuration(psutil)
        available_cpus = available_resources(psutil)["cpus"]
        with operation(emit, "stage_threads"):
            torch.set_num_threads(max(1, math.isqrt(available_cpus)))
        self.output_rate = self.rate
        with operation(emit, "stage_build"):
            try:
                self.network = self._build().to(self.device, dtype=self.dtype)
            except (RuntimeError, MemoryError) as exc:
                if self.device.type == "cpu":
                    raise
                self.emit("notice", key="device_fallback", reason=concise_error(exc))
                self.device = torch.device("cpu")
                self.network = self._build().to(self.device, dtype=self.dtype)
        self.performance_section = self.performance_identity()
        with operation(emit, "stage_restore"):
            self.restore()
            self.network.eval().requires_grad_(False)
        with operation(emit, "stage_release"):
            self.refresh_release()
            if self.playback_ready and self.release_root != self.state and not self.steps and not self.dialogue_trained:
                self.network = copy.deepcopy(self.serving_network).eval().requires_grad_(False)
                self.steps = int(self.serving_manifest.get("steps", 0))
                self.accepted = int(self.serving_manifest.get("accepted", 0))
                self._persist(self.network, self.serving_manifest.get("metrics", {}), self.cancel, increment=False, task="dialogue")
        parameters = sum(value.numel() for value in self.network.parameters())
        self.model_bytes = parameters * torch.finfo(self.dtype).bits // 8
        StorageBudget(self.state).configure(self)
        self.policy.save()
        self.emit("policy", **self.policy.effective)
        self.emit("model", name="Yuan Native Audio", device=str(self.device), dtype=str(self.dtype).replace("torch.", ""),
                  input_rate=self.rate, output_rate=self.output_rate, architecture=f"{self.width}d · {self.layers} latent audio/dialogue layers · {self.hop} samples/frame",
                  identity=self.identity()[:12], mode="ordered prompt + processed audio → Yuan latent dialogue core → autoregressive waveform", size=human_bytes(self.model_bytes), parameters=parameters,
                  accepted=self.accepted, steps=self.steps, acoustic_ready=self.acoustic_ready, dialogue_ready=self.dialogue_ready, playback_ready=self.playback_ready,
                  conversation_verified=self.dialogue_ready, dialogue_trained=self.dialogue_trained, serving_digest=self.serving_digest, capability=self.capability())

    def capability(self):
        if self.playback_ready and self.dialogue_ready:
            return "dialogue_ready"
        if self.dialogue_trained:
            return "release_wait"
        if self.acoustic_ready:
            return "acoustic_only"
        return "cold_start"

    def _device(self):
        torch = self.torch
        if torch.cuda.is_available():
            return torch.device("cuda")
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            return torch.device("mps")
        return torch.device("cpu")

    def performance_identity(self):
        return "performance:" + payload_digest([platform.system(), platform.machine(), platform.processor(), os.cpu_count(),
                                                   str(self.device), str(self.torch.__version__), self.identity(), str(self.dtype)])

    def switch_device(self, device, cancel, learner=None):
        check(cancel)
        target = self.torch.device(device)
        networks = [self.network, self.serving_network]
        if learner is not None:
            learner.checkpoint(cancel, force=True)
            learner._discard_candidate()
        for network in networks:
            if network is not None:
                network.to(target, dtype=self.dtype)
        self.device = target
        self.performance_section = self.performance_identity()
        self.capacity_scale = 1.0
        self.training_retry_at = 0.0
        self.training_oom_window = None
        self.input_budget = None
        self.calibrate(cancel, force=True)
        self.emit("model", device=str(target), performance_identity=self.performance_section)

    def _configuration(self, psutil):
        path = safe_path(self.state, "learning/model.json")
        saved = read_json(path)
        if self.owned_root is not None:
            supplied = checked_json(safe_path(self.owned_root, "learning/model.json"))
            keys = ("rate", "hop", "width", "layers")
            if (not isinstance(supplied, dict) or any(isinstance(supplied.get(key), bool) or not isinstance(supplied.get(key), int) or supplied[key] <= 0 for key in keys)
                    or supplied["hop"] > supplied["rate"]):
                raise YuanError("自有模型结构配置无效 / Invalid owned model configuration")
            supplied = {key: supplied[key] for key in keys}
            if path.exists() and (not isinstance(saved, dict) or any(saved.get(key) != supplied[key] for key in keys)):
                check(self.cancel)
                archive = safe_path(self.state, "model-states/" + payload_digest(saved) + "/" + uuid.uuid4().hex)
                archive.parent.mkdir(parents=True, exist_ok=True)
                os.replace(path.parent, archive)
                sync_dir(archive.parent)
                sync_dir(self.state)
                self.emit("deployment", key="model_configuration_selected", archived=str(archive.relative_to(self.state)), configuration=supplied)
            if not path.exists():
                atomic_json(path, supplied, self.cancel)
        if path.exists():
            saved = checked_json(path)
            try:
                values = [saved[key] for key in ("rate", "hop", "width", "layers")]
                if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in values):
                    raise ValueError("Invalid model dimensions")
                rate, hop, width, layers = values
                if hop > rate:
                    raise ValueError("Invalid model frame dimensions")
                chunk, capacity = self._execution_limits(psutil, rate, hop)
                return rate, hop, width, layers, chunk, capacity
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise YuanError("已有模型配置无效，未覆盖配置或权重 / Existing model configuration is invalid; configuration and weights retained") from exc
        source = safe_path(self.state.parent, "Yuan.model.json")
        keys = ("rate", "hop", "width", "layers")
        if os.environ.get("YUAN_PUBLISHER") == "1" and source.is_file():
            generated = checked_json(source)
            if any(isinstance(generated.get(key), bool) or not isinstance(generated.get(key), int) or generated[key] <= 0 for key in keys) or generated["hop"] > generated["rate"]:
                raise YuanError("开发端模型结构配置无效 / Invalid publisher model architecture configuration")
            generated = {key: generated[key] for key in keys}
        else:
            limits = available_resources(psutil)
            cpus, memory = limits["cpus"], limits["memory"]
            rate = self.policy.integer("model", "sample_rate", 16000, 8000, 48000)
            frame_seconds = self.policy.number("model", "frame_seconds", 0.01, 1 / rate, 0.04)
            hop = max(1, int(round(rate * frame_seconds)))
            layers = self.policy.integer("model", "layers", max(1, math.isqrt(cpus) // 2), 1, 4)
            fraction = self.policy.ratio("model", "initial_training_memory_fraction", 1 / max(16, cpus))
            budget = max(0, int(memory * fraction))
            width = 32
            def required(value):
                parameters = (32 * layers + 28) * value * value + (4 * hop + 280) * value + hop
                return parameters * 4 * 8 + rate * 4 * 8
            if required(width) > budget:
                raise YuanFault("可用内存不足以安全创建最小音频模型 / Available memory cannot safely fit the minimum audio model",
                                category="resource", code="model_memory_insufficient", stage="stage_configuration",
                                available_bytes=memory, required_bytes=math.ceil(required(width) / fraction),
                                model_budget_bytes=budget, model_required_bytes=required(width))
            while width < 256 and required(width * 2) <= budget and width * 2 <= 32 * max(1, math.isqrt(cpus)):
                width *= 2
            width = self.policy.integer("model", "width", width, 32, width)
            generated = {"rate": rate, "hop": hop, "width": width, "layers": layers}
            self.emit("deployment", key="native_model_created", automatic=True, configuration=generated,
                      available_bytes=memory, cpus=cpus, trained=False, conversation_verified=False)
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_json(path, generated, self.cancel)
        chunk, capacity = self._execution_limits(psutil, generated["rate"], generated["hop"])
        return generated["rate"], generated["hop"], generated["width"], generated["layers"], chunk, capacity

    def _execution_limits(self, psutil, rate, hop):
        resources = available_resources(psutil)
        gib = max(0.25, resources["memory"] / (1024 ** 3))
        cpu = resources["cpus"]
        baseline = max(2.0, min(math.sqrt(cpu), math.sqrt(gib) * 2))
        chunk = self.policy.number("execution", "chunk_seconds", baseline, hop / rate, max(baseline, math.sqrt(gib * cpu)))
        capacity_default = max(chunk * 3, chunk * 2 * math.sqrt(gib))
        capacity = self.policy.number("execution", "capacity_seconds", capacity_default, chunk, max(capacity_default, chunk * max(4, math.sqrt(gib * cpu))))
        return chunk, capacity

    def _build(self):
        torch, width, hop, layers = self.torch, self.width, self.hop, self.layers
        class Net(torch.nn.Module):
            def __init__(self):
                super().__init__()
                self.prompt = torch.nn.Embedding(256, width)
                self.prompt_recurrent = torch.nn.GRU(width, width, batch_first=True)
                self.prompt_mix = torch.nn.Sequential(torch.nn.Linear(width, width), torch.nn.Tanh())
                self.roles = torch.nn.Embedding(2, width)
                self.tasks = torch.nn.Embedding(2, width)
                self.frame_encoder = torch.nn.Sequential(torch.nn.Linear(hop, width), torch.nn.LayerNorm(width), torch.nn.GELU(), torch.nn.Linear(width, width), torch.nn.Tanh())
                self.audio_encoder = torch.nn.GRU(width, width, num_layers=layers, batch_first=True)
                self.memory_projection = torch.nn.Sequential(torch.nn.Linear(width, width), torch.nn.Tanh())
                self.dialogue_core = torch.nn.GRU(width, width, num_layers=layers, batch_first=True)
                self.decoder_input = torch.nn.Sequential(torch.nn.Linear(hop, width), torch.nn.Tanh())
                self.audio_decoder = torch.nn.GRU(width, width, num_layers=layers, batch_first=True)
                self.decoder_condition = torch.nn.Sequential(torch.nn.Linear(width, width), torch.nn.Tanh())
                self.output = torch.nn.Linear(width, hop)
                self.ending = torch.nn.Linear(width, 1)
                self.begin = torch.nn.Parameter(torch.zeros(1, 1, hop))
                torch.nn.init.xavier_uniform_(self.output.weight, gain=1 / math.sqrt(width))
                torch.nn.init.zeros_(self.output.bias)
                torch.nn.init.zeros_(self.ending.bias)
            def condition(self, ids, hidden=None):
                _, hidden = self.prompt_recurrent(self.prompt(ids), hidden)
                return hidden
            def initial(self, prompt_hidden, task):
                base = self.prompt_mix(prompt_hidden[-1]) + self.tasks.weight[task].reshape(1, -1)
                return base.unsqueeze(0).expand(layers, -1, -1).contiguous()
            def encode(self, frames, hidden, role, condition=None):
                values = self.frame_encoder(frames) + self.roles.weight[role].reshape(1, 1, -1)
                encoded, _ = self.audio_encoder(values)
                summary = self.memory_projection(encoded.mean(dim=1, keepdim=True))
                if condition is not None:
                    summary = summary + condition[-1].unsqueeze(1)
                _, hidden = self.dialogue_core(summary, hidden)
                return hidden
            def memory(self, frames, role):
                values = self.frame_encoder(frames) + self.roles.weight[role].reshape(1, 1, -1)
                encoded, hidden = self.audio_encoder(values)
                pooled = self.memory_projection((encoded.mean(dim=1) + hidden[-1]) / 2)
                return torch.nn.functional.normalize(pooled, dim=-1)
            def decode(self, previous, hidden, condition=None):
                inputs = self.decoder_input(previous)
                if condition is not None:
                    inputs = inputs + self.decoder_condition(condition[-1]).unsqueeze(1)
                values, hidden = self.audio_decoder(inputs, hidden)
                return torch.tanh(self.output(values)), self.ending(values).squeeze(-1), hidden
        return Net()

    @contextmanager
    def interruptible(self, cancel):
        previous = self.cancel
        self.cancel = cancel
        try:
            check(cancel)
            yield
        finally:
            self.cancel = previous

    def capacity(self):
        sample_bytes = self.np.dtype(self.np.float32).itemsize
        headroom = self.memory_headroom()
        available = min(headroom["host_available"], headroom["device_available"]) if headroom["measured"] else headroom["host_available"]
        memory_bound = available // max(sample_bytes * 24, self.width * sample_bytes)
        configured = int(self.rate * self.capacity_seconds * self.capacity_scale)
        return max(self.hop * 2, min(configured, int(memory_bound)))

    def window(self):
        return max(self.hop * 2, min(self.capacity() // 4, int(self.rate * self.chunk_seconds / 2)))

    def memory_headroom(self):
        import psutil
        host = available_resources(psutil)["memory"]
        available = host
        measured = True
        reason = None
        try:
            if self.device.type == "cuda":
                free, total = self.torch.cuda.mem_get_info(self.device)
                cached = max(0, self.torch.cuda.memory_reserved(self.device) - self.torch.cuda.memory_allocated(self.device))
                available = max(0, min(int(total), int(free + cached)))
            elif self.device.type == "mps":
                mps = self.torch.mps
                maximum = int(mps.recommended_max_memory())
                current = int(mps.current_allocated_memory())
                available = min(host, max(0, maximum - current))
        except (AttributeError, RuntimeError, OSError) as exc:
            available = 0
            measured = False
            reason = concise_error(exc)
        return {"device": str(self.device), "host_available": host, "device_available": available, "measured": measured, "reason": reason}

    def training_budget(self, learner=None):
        headroom = self.memory_headroom()
        candidate = getattr(learner, "candidate", None)
        optimizer = getattr(learner, "optimizer", None)
        resident = 0
        if candidate is not None:
            for value in candidate.parameters():
                resident += value.numel() * value.element_size()
                if value.grad is not None:
                    resident += value.grad.numel() * value.grad.element_size()
        if optimizer is not None:
            for state in optimizer.state.values():
                for value in state.values():
                    if isinstance(value, self.torch.Tensor) and value.device.type == self.device.type:
                        resident += value.numel() * value.element_size()
        copies = {"candidate": 1, "gradient": 1, "moments": 2, "promotion": 1, "restore": 1}
        fixed = max(0, self.model_bytes * sum(copies.values()) - resident)
        frames = max(self.width, math.ceil(self.window() / self.hop))
        estimate = self.width * self.layers * self.np.dtype(self.np.float32).itemsize * 72
        per_frame = self.policy.measured(getattr(self, "performance_section", "performance"), "training_bytes_per_frame", estimate)
        variable = math.ceil(frames * max(estimate, per_frame))
        required = fixed + variable
        host_required = self.model_bytes * 2 + self.window() * self.np.dtype(self.np.float32).itemsize * 4
        cooling = time.monotonic() < self.training_retry_at
        return {**headroom, "fixed_bytes": fixed, "activation_bytes": variable, "required_bytes": required, "host_required_bytes": host_required,
                "minimum_bytes": fixed + self.width * max(estimate, per_frame), "cooldown": cooling,
                "ready": bool(headroom["measured"] and headroom["device_available"] >= required and headroom["host_available"] >= host_required and not cooling)}

    def recover_training_memory(self, learner):
        import gc
        previous = self.window()
        unchanged = self.training_oom_window == previous
        self.training_oom_window = previous
        learner.candidate = learner.optimizer = None
        learner.dirty = False
        gc.collect()
        try:
            if self.device.type == "cuda":
                self.torch.cuda.empty_cache()
            elif self.device.type == "mps":
                self.torch.mps.empty_cache()
        except (AttributeError, RuntimeError):
            pass
        minimum = self.hop * 2
        base_capacity = max(minimum, int(self.rate * self.capacity_seconds))
        floor = min(1.0, minimum * 4 / base_capacity)
        self.capacity_scale = max(floor, self.capacity_scale / math.sqrt(2))
        current = self.window()
        bounded = current >= previous or unchanged
        delay = self.policy.number("learning", "memory_retry_seconds", max(self.chunk_seconds, math.sqrt(self.capacity_seconds)), self.hop / self.rate, max(self.chunk_seconds, self.capacity_seconds))
        self.training_retry_at = time.monotonic() + delay if bounded else 0.0
        self.emit("learning", key="memory_fixed_pause" if bounded else "memory_reduced", previous_window=previous, window=current, device=str(self.device), retry_seconds=delay if bounded else 0.0, **learner._state())

    def normalize(self, waveform, rate):
        import soxr
        np = self.np
        waveform = np.asarray(waveform, dtype=np.float32)
        if waveform.ndim == 2:
            waveform = waveform.mean(axis=-1)
        if waveform.ndim != 1 or not waveform.size or rate <= 0 or not np.isfinite(waveform).all():
            raise YuanError("输入音频无效 / Invalid input audio")
        if int(rate) != self.rate:
            waveform = soxr.resample(waveform, int(rate), self.rate).astype(np.float32)
        waveform = waveform - waveform.mean(dtype=np.float64)
        peak = float(np.max(np.abs(waveform)))
        if peak > 1:
            waveform = waveform / peak
        if not waveform.size or not np.isfinite(waveform).all():
            raise YuanError("处理后的音频含无效数值 / Processed audio contains invalid values")
        return np.ascontiguousarray(waveform, dtype=np.float32)

    def _prompt_ids(self, prompt):
        import psutil
        data = str(prompt).encode("utf-8") or b"\0"
        budget = max(self.width, available_resources(psutil)["memory"] // max(1, self.width * self.torch.tensor([], dtype=self.dtype).element_size() * 16))
        if len(data) > budget:
            raise YuanError("提示词超过当前内存预算 / Instructions exceed the current memory budget")
        values = self.np.frombuffer(data, dtype=self.np.uint8).astype(self.np.int64)
        return self.torch.from_numpy(values).to(self.device).reshape(1, -1)

    def _frames(self, waveform):
        torch = self.torch
        np = self.np
        values = np.asarray(waveform, dtype=np.float32).reshape(-1)
        missing = (-len(values)) % self.hop
        if missing:
            values = np.pad(values, (0, missing))
        tensor = torch.from_numpy(values.copy()).to(self.device, dtype=self.dtype).reshape(1, -1, self.hop)
        return tensor, len(waveform)

    def audio_blocks(self, waveform, cancel=None):
        step = max(self.hop, min(self.width * self.hop, self.window() // self.hop * self.hop))
        if isinstance(waveform, AudioSequence):
            yield from waveform.blocks(step, cancel)
        else:
            values = self.np.asarray(waveform, dtype=self.np.float32)
            if values.ndim != 1 or not self.np.isfinite(values).all():
                raise InvalidAudio("模型输入音频无效 / Invalid model audio input")
            for start in range(0, len(values), step):
                check(cancel)
                yield values[start:start + step]

    def _encode_audio(self, network, waveform, hidden, role, cancel, pending=None, final=True, condition=None):
        np, torch = self.np, self.torch
        pending = np.empty(0, dtype=np.float32) if pending is None else pending
        total = len(waveform) + len(pending)
        consumed = 0
        block_samples = self.width * self.hop
        gradient_window = max(block_samples, self.window() // self.hop * self.hop)
        training = torch.is_grad_enabled()
        for values in self.audio_blocks(waveform, cancel):
            check(cancel)
            values = np.concatenate((pending, values)) if len(pending) else values
            count = len(values) // block_samples * block_samples
            pending = values[count:].copy()
            if not count:
                continue
            consumed += count
            track = training and consumed > max(0, total - gradient_window)
            with torch.set_grad_enabled(track):
                if not track:
                    hidden = hidden.detach()
                frames, _ = self._frames(values[:count])
                hidden = network.encode(frames, hidden, int(role == "assistant"), condition)
        if final and len(pending):
            check(cancel)
            frames, _ = self._frames(pending)
            hidden = network.encode(frames, hidden, int(role == "assistant"), condition)
            pending = np.empty(0, dtype=np.float32)
        return hidden, pending

    def _prompt_condition(self, network, prompt, cancel, task):
        ids = self._prompt_ids(prompt)
        prompt_hidden = None
        step = max(1, self.width)
        training = self.torch.is_grad_enabled()
        for start in range(0, ids.shape[1], step):
            check(cancel)
            track = training and start + step >= ids.shape[1]
            with self.torch.set_grad_enabled(track):
                prompt_hidden = network.condition(ids[:, start:start + step], prompt_hidden)
        return network.initial(prompt_hidden, int(task == "dialogue"))

    def _encode(self, network, prompt, waveform, context=(), cancel=None, task="dialogue", condition=None, recalled=(), incoming_count=None, context_id=None):
        condition = self._prompt_condition(network, prompt, cancel, task) if condition is None else condition
        hidden = condition
        context = self._history_budget(context, len(waveform) if incoming_count is None else incoming_count, recalled, context_id)
        for role, audio in (*context, ("user", waveform)):
            if len(audio):
                hidden, pending = self._encode_audio(network, audio, hidden, role, cancel, condition=condition)
        return hidden

    @supervised('memory_encoding', 'runtime')
    def memory_vector(self, waveform, role="user", cancel=None):
        np, torch = self.np, self.torch
        if not len(waveform):
            raise InvalidAudio("音频记忆输入为空 / Audio memory input is empty")
        block_samples = max(1, self.width) * self.hop
        pending = np.empty(0, dtype=np.float32)
        total = None
        count = 0
        network = self.serving_network if self.playback_ready and self.serving_network is not None else self.network
        network.eval()
        with self.interruptible(cancel), torch.inference_mode():
            for values in self.audio_blocks(waveform, cancel):
                check(cancel)
                values = np.concatenate((pending, values)) if len(pending) else values
                complete = len(values) // block_samples * block_samples
                for start in range(0, complete, block_samples):
                    check(cancel)
                    frames, _ = self._frames(values[start:start + block_samples])
                    vector = network.memory(frames, int(role == "assistant"))
                    total = vector if total is None else total + vector
                    count += 1
                pending = values[complete:].copy()
            if len(pending):
                check(cancel)
                frames, _ = self._frames(pending)
                vector = network.memory(frames, int(role == "assistant"))
                total = vector if total is None else total + vector
                count += 1
            if total is None:
                raise InvalidAudio("音频记忆输入为空 / Audio memory input is empty")
            vector = torch.nn.functional.normalize(total / count, dim=-1)
            values = vector.reshape(-1).detach().float().cpu().numpy().copy()
        if not values.size or not np.isfinite(values).all():
            raise YuanError("音频记忆向量无效 / Invalid audio memory vector")
        return np.ascontiguousarray(values, dtype=np.float32)

    def reply_samples(self, incoming, history=()):
        capacity = self.capacity()
        fraction = self.policy.ratio("inference", "reply_capacity_fraction", 0.5)
        seconds = self.policy.number("inference", "reply_max_seconds", capacity * fraction / self.rate, self.hop / self.rate, capacity / self.rate)
        return max(self.hop, min(capacity, int(seconds * self.rate / self.hop) * self.hop))

    def _decode(self, network, hidden, count, cancel=None, stop=True, phase=False, previous=None, condition=None):
        torch = self.torch
        previous = network.begin if previous is None else previous
        condition = hidden if condition is None else condition
        pieces = []
        total = max(1, math.ceil(count / self.hop))
        last = 0.0
        stopped = False
        for frame in range(total):
            check(cancel)
            predicted, ending, hidden = network.decode(previous, hidden, condition)
            if not torch.isfinite(predicted).all() or not torch.isfinite(ending).all():
                raise YuanError("模型输出含无效数值 / Model output contains invalid values")
            pieces.append(predicted)
            previous = predicted
            now = time.monotonic()
            if phase and (now - last >= math.sqrt(self.hop / self.rate) or frame + 1 == total):
                self.emit("phase", key="generating", current=frame + 1, total=total, unit="frames")
                last = now
            if stop and ending.item() > 0:
                stopped = True
                break
        if phase and stop:
            self.emit("generation_end", key="natural_end" if stopped else "limited", natural=stopped, limited=not stopped, frames=len(pieces), samples=min(count, len(pieces) * self.hop))
            if not stopped:
                self.emit("notice", key="limited", component="generation")
        return torch.cat(pieces, dim=1).reshape(1, -1)[:, :count]

    def respond_audio(self, prompt, history, waveform, cancel, max_samples=None, network=None):
        pieces = list(self.stream_audio(prompt, history, waveform, cancel, max_samples=max_samples, network=network))
        check(cancel)
        if not pieces:
            raise YuanError("模型未产生有效音频 / Model produced no valid audio")
        return self.np.ascontiguousarray(self.np.concatenate(pieces), dtype=self.np.float32)

    def _acoustic_loss(self, prediction, target):
        torch = self.torch
        waveform = torch.nn.functional.smooth_l1_loss(prediction, target)
        motion = torch.nn.functional.smooth_l1_loss(prediction[:, 1:] - prediction[:, :-1], target[:, 1:] - target[:, :-1]) if target.shape[1] > 1 else waveform * 0
        spectra = []
        maximum = min(target.shape[1], self.hop * max(2, self.layers + 1))
        size = self.hop
        while size <= maximum:
            window = torch.hann_window(size, device=prediction.device, dtype=prediction.dtype)
            left = prediction.unfold(-1, size, max(1, size // 2)) * window
            right = target.unfold(-1, size, max(1, size // 2)) * window
            left = torch.log1p(torch.abs(torch.fft.rfft(left, dim=-1)))
            right = torch.log1p(torch.abs(torch.fft.rfft(right, dim=-1)))
            spectra.append(torch.nn.functional.smooth_l1_loss(left, right))
            size *= 2
        spectrum = sum(spectra) / len(spectra) if spectra else waveform * 0
        return waveform + motion + spectrum

    def loss(self, network, prompt, source, target, ended=False, context=(), rollout=False, task="acoustic", prefix=None, end_label_valid=False):
        torch = self.torch
        check(self.cancel)
        n = min(len(target), self.window())
        if n < self.hop or len(source) < self.hop:
            raise YuanError("音频样本过短，无法学习 / Audio sample is too short to learn from")
        condition = self._encode(network, prompt, source, context, self.cancel, task=task)
        hidden = condition
        seed = network.begin
        if prefix is not None:
            with torch.no_grad():
                for audio in prefix():
                    check(self.cancel)
                    if len(audio) % self.hop:
                        raise YuanError("回复前缀没有按帧对齐 / Reply prefix is not frame aligned")
                    prefix_frames, _ = self._frames(audio)
                    previous_frames = torch.cat((seed, prefix_frames[:, :-1]), dim=1)
                    for offset in range(0, previous_frames.shape[1], max(1, self.width)):
                        check(self.cancel)
                        _, _, hidden = network.decode(previous_frames[:, offset:offset + self.width], hidden, condition.detach())
                    seed = prefix_frames[:, -1:]
                hidden = hidden.detach()
                seed = seed.detach()
        target_frames, _ = self._frames(target[:n])
        previous = torch.cat((seed, target_frames[:, :-1]), dim=1)
        predictions = []
        endings = []
        step = max(1, self.width)
        current_hidden = hidden
        for start in range(0, previous.shape[1], step):
            check(self.cancel)
            predicted, ending, current_hidden = network.decode(previous[:, start:start + step], current_hidden, condition)
            predictions.append(predicted)
            endings.append(ending)
        prediction = torch.cat(predictions, dim=1).reshape(1, -1)[:, :n]
        target_tensor = target_frames.reshape(1, -1)[:, :n]
        loss = self._acoustic_loss(prediction, target_tensor)
        if end_label_valid:
            end_logits = torch.cat(endings, dim=1)
            end_target = torch.zeros_like(end_logits)
            if ended and n == len(target):
                end_target[:, -1] = 1
            end_loss = torch.nn.functional.binary_cross_entropy_with_logits(end_logits, end_target)
            loss = loss + end_loss / math.sqrt(max(1, n / self.hop))
        if rollout:
            free = self._decode(network, hidden, n, self.cancel, stop=False, previous=seed, condition=condition)
            loss = loss + self._acoustic_loss(free, target_tensor)
        return loss

    def clone(self):
        return copy.deepcopy(self.network).to(self.device, dtype=self.dtype)

    def identity(self):
        payload = ["yuan-owned-latent-dialogue-waveform-v5", self.rate, self.hop, self.width, self.layers]
        return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()

    def _load_network(self, manifest, cancel=None, state=None):
        if not isinstance(manifest, dict) or manifest.get("identity") != self.identity() or not is_digest(manifest.get("sha256")):
            raise YuanError("模型结构或摘要不匹配，原文件已保留 / Model architecture or digest mismatch; originals retained")
        directory = safe_path(self.state if state is None else state, "learning")
        path = safe_path(directory, manifest["file"])
        if path.suffix != ".pt" or digest_file(path, cancel) != manifest["sha256"]:
            raise YuanError("模型完整性校验失败 / Model integrity validation failed")
        weights = self.torch.load(str(path), map_location="cpu", weights_only=True)
        if not isinstance(weights, dict) or not weights or not all(isinstance(value, self.torch.Tensor) and self.torch.isfinite(value).all() for value in weights.values()):
            raise YuanError("模型权重无效 / Invalid model weights")
        network = self._build()
        network.load_state_dict(weights, strict=True)
        return network.to(self.device, dtype=self.dtype).eval().requires_grad_(False)

    def restore(self):
        directory = safe_path(self.state, "learning")
        directory.mkdir(parents=True, exist_ok=True)
        self.training_manifest = None
        if self._resume_training_activation(cancel=self.cancel):
            return
        failures = []
        for name in ("active.json", "previous.json"):
            path = directory / name
            if not path.exists():
                continue
            try:
                manifest = checked_json(path)
                network = self._load_network(manifest, self.cancel)
                steps, accepted = manifest.get("steps", 0), manifest.get("accepted", 0)
                if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in (steps, accepted)):
                    raise YuanError("模型训练计数无效 / Invalid model training counters")
                if name != "active.json":
                    current = directory / "active.json"
                    atomic_json(current, manifest, self.cancel)
                    self.emit("notice", key="model_recovered", file=manifest["file"], failures=failures)
                self.network = network
                self.steps, self.accepted = steps, accepted
                self.acoustic_ready = manifest.get("acoustic_ready") is True
                self.dialogue_trained = manifest.get("dialogue_trained") is True
                self.active_digest = manifest["sha256"]
                self.training_manifest = manifest
                return
            except Cancelled:
                raise
            except Exception as exc:
                failures.append(concise_error(exc))
                self.emit("notice", key="checkpoint_failure", file=name, reason=failures[-1])
        if failures or any(directory.glob("model-*.pt")):
            raise YuanError("没有可安全恢复的模型，未重置或删除已有权重 / No model can be safely restored; existing weights were not reset or deleted")
        if self.owned_root is None:
            self._persist(self.network, {}, self.cancel, increment=False, task=None)

    @supervised('model_persist', 'learning')
    def _persist(self, network, metrics, cancel, increment, task, store=None):
        if store is not None and store.db.in_transaction:
            raise YuanError("模型文件必须在数据库写事务之外保存 / Model files must be saved outside database write transactions")
        directory = safe_path(self.state, "learning")
        directory.mkdir(parents=True, exist_ok=True)
        name = "model-" + uuid.uuid4().hex + ".pt"
        target = directory / name
        temporary = target.with_suffix(".partial")
        sidecar = target.with_suffix(".json")
        check(cancel)
        committed = False
        baseline = self.active_digest
        previous = getattr(self, "training_manifest", None)
        acoustic_ready = self.acoustic_ready or task in ("acoustic", "dialogue")
        dialogue_trained = self.dialogue_trained or task == "dialogue"
        with StorageBudget(self.state).reserve(max(1, sum(value.numel() * value.element_size() for value in network.parameters()) * 2), "checkpoint"):
            try:
                saved = {key: value.detach().cpu().contiguous() for key, value in network.state_dict().items()}
                if not all(self.torch.isfinite(value).all() for value in saved.values()):
                    raise YuanError("拒绝保存无效模型权重 / Refusing to save invalid model weights")
                self.torch.save(saved, str(temporary))
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                check(cancel)
                verified = self.torch.load(str(temporary), map_location="cpu", weights_only=True)
                if set(verified) != set(saved) or any(not self.torch.equal(verified[key], value) for key, value in saved.items()):
                    raise YuanError("模型写入后校验失败 / Model post-write verification failed")
                os.replace(temporary, target)
                sync_dir(directory)
                accepted = self.accepted + int(bool(increment))
                checksum = digest_file(target, cancel)
                manifest = {"identity": self.identity(), "file": name, "sha256": checksum, "steps": self.steps,
                            "accepted": accepted, "metrics": metrics, "acoustic_ready": acoustic_ready,
                            "dialogue_trained": dialogue_trained, "created": time.time()}
                prior = previous or getattr(self, "serving_manifest", {})
                prior_provenance = prior.get("training_provenance", {}) if isinstance(prior, dict) else {}
                manifest["training_provenance"] = {"owner": APP_NAME, "self_trained": bool(task in ("acoustic", "dialogue") and self.steps > 0 or prior_provenance.get("self_trained") is True),
                                                   "external_model": False, "text_to_speech": False,
                                                   "origin": "yuan-native-network", "parent_model": prior.get("sha256") if isinstance(prior, dict) else None,
                                                   "build_sha256": digest_file(Path(__file__), cancel), "data_revision": metrics.get("data_revision"),
                                                   "lineage_sha256": payload_digest([prior_provenance.get("lineage_sha256"), checksum, self.steps, metrics])}
                atomic_json(sidecar, manifest, cancel)
                if store is not None:
                    check(cancel)
                    with store.transaction():
                        if store.setting("data_revision") != metrics.get("data_revision") or self.active_digest != baseline:
                            return False
                        if store.setting("pending_training_activation"):
                            raise YuanError("上次模型更新尚未完成恢复 / The previous model update still needs recovery")
                        store.set_setting("pending_training_activation", {"manifest": manifest, "previous": previous})
                    committed = True
                self._publish_training(manifest, previous, network, cancel)
                committed = True
                if store is not None:
                    store.set_setting("pending_training_activation", None)
            finally:
                temporary.unlink(missing_ok=True)
                if not committed:
                    target.unlink(missing_ok=True)
                    sidecar.unlink(missing_ok=True)
            try:
                self._prune_models()
            except (OSError, ValueError, TypeError, YuanError) as exc:
                self.emit("notice", reason=concise_error(exc), component="checkpoint_cleanup")
        return True

    def _publish_training(self, manifest, previous, network, cancel):
        steps, accepted = manifest.get("steps"), manifest.get("accepted")
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in (steps, accepted)):
            raise YuanError("模型训练计数无效 / Invalid model training counters")
        directory = safe_path(self.state, "learning")
        if previous:
            atomic_json(directory / "previous.json", previous, cancel)
        atomic_json(directory / "active.json", manifest, cancel)
        self.network = network
        self.training_manifest = manifest
        self.steps, self.accepted = steps, accepted
        self.acoustic_ready = manifest.get("acoustic_ready") is True
        self.dialogue_trained = manifest.get("dialogue_trained") is True
        self.active_digest = manifest["sha256"]

    def _resume_training_activation(self, store=None, cancel=None):
        owns_store = store is None
        if owns_store:
            if not safe_path(self.state, "memory/memory.sqlite3").is_file():
                return False
            store = AudioStore(self.state, recover=False)
        try:
            record = store.setting("pending_training_activation")
            if not record:
                return False
            if not isinstance(record, dict) or not isinstance(record.get("manifest"), dict):
                raise YuanError("模型提交记录无效，已保留原文件 / Invalid model commit record; originals retained")
            manifest = record["manifest"]
            network = self._load_network(manifest, cancel)
            self._publish_training(manifest, record.get("previous"), network, cancel)
            with store.transaction():
                if store.setting("pending_training_activation") != record:
                    raise YuanError("恢复期间模型提交记录发生变化 / Model commit record changed during recovery")
                store.set_setting("pending_training_activation", None)
            self.emit("notice", key="promotion_recovered", model=self.active_digest, serving_model=self.serving_digest)
            return True
        finally:
            if owns_store:
                store.close()

    def _prune_models(self):
        directory = safe_path(self.state, "learning")
        protected = set()
        for name in ("active.json", "previous.json"):
            manifest = read_json(directory / name)
            if manifest.get("file"):
                protected.add(manifest["file"])
        for name in ("release.json", "pending-review.json", "previous-release.json"):
            pointer = read_json(directory / name)
            try:
                evaluation = read_json(safe_path(directory, pointer["evaluation_file"]))
                protected.add(evaluation["model"]["file"])
            except (KeyError, TypeError, YuanError):
                pass
        keep = self.policy.integer("learning", "retained_models", 2, 2, max(2, self.layers * 4))
        manifests = []
        for path in directory.glob("model-*.json"):
            manifest = read_json(path)
            if manifest.get("identity") == self.identity() and manifest.get("file"):
                manifests.append((float(manifest.get("created", 0)), path, manifest))
        for _, _, manifest in sorted(manifests, key=lambda value: value[0], reverse=True)[:keep]:
            protected.add(manifest["file"])
        for _, path, manifest in manifests:
            if manifest["file"] not in protected:
                try:
                    safe_path(directory, manifest["file"]).unlink(missing_ok=True)
                    path.unlink(missing_ok=True)
                except (OSError, YuanError) as exc:
                    self.emit("notice", reason=concise_error(exc), component="checkpoint_cleanup")

    def _prune_evaluations(self, cancel=None):
        directory = safe_path(self.state, "learning")
        root = safe_path(directory, "evaluations")
        if not root.is_dir() or root.is_symlink():
            return
        try:
            protected = set()
            for name in ("release.json", "pending-review.json", "previous-release.json"):
                pointer_path = directory / name
                if not pointer_path.exists():
                    continue
                pointer = checked_json(pointer_path)
                path = safe_path(directory, pointer["evaluation_file"])
                relative = path.relative_to(root)
                protected.add(relative.parts[0])
            candidates = []
            total = 0
            for path in root.iterdir():
                check(cancel)
                if path.is_symlink() or not path.is_dir() or len(path.name) != len(uuid.uuid4().hex) or any(value not in "0123456789abcdef" for value in path.name):
                    continue
                files = list(path.rglob("*"))
                if any(file.is_symlink() for file in files):
                    continue
                size = sum(file.stat().st_size for file in files if file.is_file())
                total += size
                if path.name not in protected:
                    candidates.append((path.stat().st_mtime_ns, path, size))
            keep = self.policy.integer("storage", "retained_evaluations", max(1, self.layers), 0, max(1, self.width))
            free = shutil.disk_usage(self.state).free
            budget = self.policy.integer("storage", "evaluation_cache_bytes", max(0, (free + total) // 4), 0, max(1, free + total))
            remaining = len(candidates)
            removed = reclaimed = 0
            for _, path, size in sorted(candidates, key=lambda row: row[0]):
                if remaining <= keep and total <= budget:
                    break
                check(cancel)
                shutil.rmtree(path)
                remaining -= 1
                removed += 1
                reclaimed += size
                total -= size
            if removed:
                self.emit("storage_cleanup", key="evaluation_cleanup", removed=removed, reclaimed_bytes=reclaimed, protected=len(protected), retained_bytes=total)
        except Cancelled:
            raise
        except (OSError, ValueError, KeyError, TypeError, YuanError) as exc:
            self.emit("notice", key="evaluation_cleanup", reason=concise_error(exc), protected=True)

    def _review_case(self, store, case):
        pair = store.db.execute("SELECT p.*,g.split,g.source_key FROM turn_pairs p JOIN groups g ON g.id=p.group_id WHERE p.id=?", (case["pair"],)).fetchone()
        if pair is None or pair["split"] != "release" or not pair["verified"]:
            raise YuanError("听测样本不是独立发布留出配对 / Listening case is not an independent release-held-out pair")
        user, assistant = store.sample(pair["user_sample"]), store.sample(pair["assistant_sample"])
        evidence = json.loads(pair["evidence"])
        if (user is None or assistant is None or user["generated"] or assistant["generated"]
                or not user["dialogue_eligible"] or not assistant["dialogue_eligible"]
                or evidence.get("scenarios", []) != case.get("scenarios", [])
                or evidence.get("language") != case.get("language") or evidence.get("prompt") != case.get("prompt")
                or user["digest"] != case.get("user_digest") or assistant["digest"] != case.get("target_digest")
                or case.get("input_samples") != user["count"] or case.get("input_start") != 0 or case.get("input_end") != user["count"]
                or pair["source_key"] != case.get("recording_id") or payload_digest(evidence) != case.get("annotation_digest")):
            raise YuanError("听测数据来源或标注发生变化 / Listening dataset provenance or annotations changed")
        for row in (user, assistant):
            store.read_audio(row, 0, min(row["count"], self.hop), cancel=self.cancel, verify=True)
        history = case.get("history")
        if not isinstance(history, list) or "history" in evidence.get("scenarios", []) and not history:
            raise YuanError("听测历史音频已改变或缺失 / Listening history audio has changed or is missing")
        seen = set()
        previous = None
        for item in history:
            row = store.sample(item.get("id")) if isinstance(item, dict) else None
            if (row is None or row["id"] in seen or row["group_id"] != pair["group_id"] or row["generated"] or not row["dialogue_eligible"]
                    or row["id"] in (user["id"], assistant["id"]) or row["created"] >= user["created"] or row["digest"] != item.get("sha256") or row["role"] != item.get("role")):
                raise YuanError("听测历史来源无效 / Invalid listening history provenance")
            order = (row["created"], row["id"])
            start, samples = item.get("start"), item.get("samples")
            if (previous is not None and order < previous or isinstance(start, bool) or not isinstance(start, int) or start < 0
                    or isinstance(samples, bool) or not isinstance(samples, int) or samples <= 0 or start + samples > row["count"]):
                raise YuanError("听测历史顺序或范围无效 / Invalid listening history order or range")
            store.read_audio(row, start, min(samples, self.hop), cancel=self.cancel, verify=True)
            seen.add(row["id"])
            previous = order
        return pair

    def review_history(self, store, pair, cancel):
        user = store.sample(pair["user_sample"])
        rows = store.db.execute("SELECT * FROM samples WHERE group_id=? AND created<? AND generated=0 AND dialogue_eligible=1 AND id NOT IN (?,?) ORDER BY created DESC,id DESC",
                                (pair["group_id"], user["created"], pair["user_sample"], pair["assistant_sample"])).fetchall()
        remaining = max(0, self.capacity() // 2)
        selected = []
        for row in rows:
            check(cancel)
            if remaining <= 0:
                break
            take = min(row["count"], self.window(), remaining)
            if take <= 0:
                continue
            start = row["count"] - take
            audio = store.read_audio(row, start, take, cancel=cancel, verify=True)
            selected.append(((row["role"], audio), {"id": row["id"], "sha256": row["digest"], "role": row["role"], "start": start, "samples": take}))
            remaining -= len(audio)
        selected.reverse()
        return tuple(item[0] for item in selected), [item[1] for item in selected]

    def refresh_release(self):
        directory = safe_path(self.state, "learning")
        current = directory / "release.json"
        valid = False
        deployment = read_json(safe_path(self.state, "owned-release.json"), {})
        superseded = deployment.get("superseded_local_review_sha256")
        if current.exists() and (not superseded or digest_file(current, self.cancel) != superseded):
            try:
                valid = self._load_review(current)
            except Cancelled:
                raise
            except Exception as exc:
                self._release_error(exc, candidate=False)
        elif self.owned_root is None:
            self.dialogue_ready = self.playback_ready = False
            self.serving_network = self.serving_digest = None
            self.release_signature = None
        if not valid and self.owned_root is not None:
            try:
                valid = self._load_review(safe_path(self.owned_root, "learning/release.json"), state=self.owned_root)
            except Cancelled:
                raise
            except Exception as exc:
                self._release_error(exc, candidate=False)
        try:
            pointer = read_json(directory / "pending-review.json")
            if not pointer.get("review_file"):
                return valid
            path = safe_path(directory, pointer["review_file"])
            draft = read_json(path)
            required = ("understandable", "follows_prompt", "relevant_reply", "ends_normally", "no_abnormal_audio")
            checks = draft.get("checks", [])
            complete = bool(checks) and all(isinstance(row, dict) and all(row.get(key) is True for key in required) for row in checks)
            if not complete:
                return valid
            evaluation = checked_json(safe_path(directory, draft["evaluation_file"]))
            if valid and evaluation.get("model", {}).get("sha256") == self.serving_digest:
                return valid
            return self._load_review(path, pending=True)
        except Cancelled:
            raise
        except Exception as exc:
            self._release_error(exc, candidate=True)
            return valid

    def _release_error(self, exc, candidate=False):
        if not candidate:
            self.dialogue_ready = self.playback_ready = False
            self.serving_network = self.serving_digest = None
            self.release_signature = None
        reason = concise_error(exc)
        key = "candidate_review_failure" if candidate else "release_failure"
        if reason != getattr(self, key, None):
            self.emit("notice", key="release_invalid", candidate=candidate, serving_preserved=bool(candidate and self.playback_ready), reason=reason)
        setattr(self, key, reason)

    def _load_review(self, path, pending=False, state=None, announce=True):
        state = self.state if state is None else Path(state)
        directory = safe_path(state, "learning")
        signature = (str(path.resolve()), digest_file(path, self.cancel))
        if not pending and signature == self.release_signature and self.serving_network is not None:
            unchanged = True
            for file, previous in self.release_files.items():
                stat = Path(file).stat()
                if (stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size, stat.st_dev, stat.st_ino) != previous:
                    unchanged = False
                    break
            if unchanged:
                store = AudioStore(state, recover=False)
                try:
                    groups = [self._review_case(store, case)["group_id"] for case in self.release_cases]
                    store.assert_independent(groups)
                finally:
                    store.close()
                return self.playback_ready
        review = checked_json(path)
        if review.get("protocol") != "yuan-native-listening-v3" or not isinstance(review.get("reviewer"), str) or not review["reviewer"].strip():
            raise YuanError("听测验收协议或评审人无效 / Invalid listening acceptance protocol or reviewer")
        evaluation_path = safe_path(directory, review["evaluation_file"])
        if digest_file(evaluation_path, self.cancel) != review.get("evaluation_sha256"):
            raise YuanError("听测结果摘要不匹配 / Listening evaluation digest mismatch")
        evaluation = checked_json(evaluation_path)
        if evaluation.get("protocol") != review["protocol"] or evaluation.get("evaluator_sha256") != behavior_digest() or not is_digest(evaluation.get("build_sha256")) or evaluation.get("identity") != self.identity():
            raise YuanError("听测程序版本或模型结构不匹配 / Listening evaluator version or model architecture mismatch")
        reviewed_at = review.get("reviewed_at")
        if isinstance(reviewed_at, bool) or not isinstance(reviewed_at, (float, int)) or not math.isfinite(reviewed_at) or reviewed_at < evaluation["created"]:
            raise YuanError("听测验收时间无效 / Invalid listening review time")
        cases = evaluation.get("cases", [])
        checks = review.get("checks", [])
        if not cases or not isinstance(checks, list) or len(checks) != len(cases):
            raise YuanError("听测验收项目不完整 / Listening review cases are incomplete")
        approved = {item["id"]: item for item in checks}
        if len(approved) != len(cases) or set(approved) != {case["id"] for case in cases}:
            raise YuanError("听测验收项目重复或不匹配 / Duplicate or mismatched listening cases")
        required = ("understandable", "follows_prompt", "relevant_reply", "ends_normally", "no_abnormal_audio")
        if any(any(approved[case["id"]].get(key) is not True for key in required) for case in cases):
            raise YuanError("听测验收尚未全部通过 / Listening acceptance has not passed all checks")
        dataset = [{key: case[key] for key in ("id", "pair", "language", "prompt", "recording_id", "user_digest", "target_digest", "annotation_digest", "input_samples", "input_start", "input_end", "scenarios", "history")} for case in cases]
        if payload_digest(dataset) != evaluation.get("dataset_sha256"):
            raise YuanError("听测数据集版本不匹配 / Listening dataset version mismatch")
        acceptance = AcceptancePolicy(self)
        if evaluation.get("acceptance_policy_sha256") != acceptance.digest:
            raise YuanError("验收策略与听测记录不匹配 / Acceptance policy does not match the listening record")
        acceptance.check_coverage(cases)
        files = [path, evaluation_path, Path(__file__), acceptance.path]
        store = AudioStore(state, recover=False)
        try:
            groups = [self._review_case(store, case)["group_id"] for case in cases]
            store.assert_independent(groups)
        finally:
            store.close()
        import soundfile as sf
        for case in cases:
            check(self.cancel)
            for field in ("input", "output"):
                audio_path = safe_path(directory, case[field + "_file"])
                if digest_file(audio_path, self.cancel) != case[field + "_sha256"]:
                    raise YuanError("听测音频完整性校验失败 / Listening audio integrity validation failed")
                with sf.SoundFile(str(audio_path)) as audio_file:
                    if audio_file.samplerate != self.rate or audio_file.channels != 1 or audio_file.frames <= 0 or field == "input" and audio_file.frames != case["input_samples"]:
                        raise YuanError("听测音频无效 / Invalid listening audio")
                    for values in audio_file.blocks(blocksize=max(self.hop, self.window()), dtype="float32"):
                        check(self.cancel)
                        if not self.np.isfinite(values).all() or float(self.np.max(self.np.abs(values))) > 1:
                            raise YuanError("听测音频无效 / Invalid listening audio")
                files.append(audio_path)
        manifest = evaluation["model"]
        if manifest.get("dialogue_trained") is not True:
            raise YuanError("模型尚未通过对话训练验证 / Model has not passed dialogue training validation")
        network = self._load_network(manifest, self.cancel, state=state)
        files.append(safe_path(directory, manifest["file"]))
        next_files = {}
        for file in files:
            stat = file.stat()
            next_files[str(file)] = (stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size, stat.st_dev, stat.st_ino)
        if pending:
            atomic_json(directory / "release.json", review, self.cancel)
            path = directory / "release.json"
            try:
                (directory / "pending-review.json").unlink(missing_ok=True)
                sync_dir(directory)
            except OSError as exc:
                self.emit("notice", component="release_pointer", reason=concise_error(exc))
        self.serving_network, self.serving_digest = network, manifest["sha256"]
        self.serving_manifest = manifest
        self.release_root = state
        self.dialogue_ready = self.playback_ready = True
        self.release_signature = signature
        self.release_files = next_files
        self.release_failure = None
        self.release_cases = cases
        if announce:
            self.record_serving()
        return True

    def record_serving(self):
        release_state = read_json(safe_path(self.state, "serving-state.json"), {})
        if release_state.get("model_sha256") != self.serving_digest:
            atomic_json(safe_path(self.state, "serving-state.json"), {"model_sha256": self.serving_digest, "versions": int(release_state.get("versions", 0)) + 1,
                                                                   "accepted_at": time.time()}, self.cancel)
        self.emit("model", key="release_loaded", dialogue_ready=True, playback_ready=True, conversation_verified=True, serving_digest=self.serving_digest, capability=self.capability())

    @supervised('review_generation', 'learning')
    def prepare_review(self, store, cancel):
        if not self.dialogue_trained:
            return False
        directory = safe_path(self.state, "learning")
        self._prune_evaluations(cancel)
        held = getattr(self, "review_held", None)
        for name in ("pending-review.json", "release.json"):
            pointer = read_json(directory / name)
            try:
                evaluation_file = pointer.get("evaluation_file")
                evaluated = read_json(safe_path(directory, evaluation_file)) if evaluation_file else {}
                held_review = bool(name == "pending-review.json" and held and held == pointer.get("model_sha256") == evaluated.get("model", {}).get("sha256"))
                if ((evaluated.get("model", {}).get("sha256") == self.active_digest or held_review) and evaluated.get("evaluator_sha256") == behavior_digest()
                        and evaluated.get("protocol") == "yuan-native-listening-v3" and evaluated.get("identity") == self.identity()):
                    cases = evaluated.get("cases", [])
                    if not cases:
                        raise YuanError("听测数据为空 / Listening cases are empty")
                    groups = [self._review_case(store, case)["group_id"] for case in cases]
                    store.assert_independent(groups)
                    return False
            except (OSError, ValueError, TypeError, KeyError, YuanError):
                pass
        self.review_held = None
        acceptance = AcceptancePolicy(self)
        policy_path = directory / "acceptance-policy.json"
        if policy_path.resolve() != acceptance.path.resolve():
            atomic_json(policy_path, acceptance.data, cancel)
        selected = acceptance.select(store, self)
        if not selected:
            return False
        store.assert_independent([row["group_id"] for row in selected])
        required = sum((store.sample(row["user_sample"])["count"] + self.capacity()) * self.np.dtype(self.np.float32).itemsize for row in selected)
        free = shutil.disk_usage(self.state).free
        reserve = max(required, getattr(self, "model_bytes", 0) * 2)
        if free < required + reserve:
            self.emit("notice", key="review_storage_pause", required_bytes=required + reserve, available_bytes=free)
            return False
        import soundfile as sf
        evaluation_id = uuid.uuid4().hex
        root = safe_path(directory, "evaluations/" + evaluation_id)
        root.mkdir(parents=True, exist_ok=True)
        cases = []
        committed = False
        try:
            for row in selected:
                check(cancel)
                evidence = json.loads(row["evidence"])
                user, target = store.sample(row["user_sample"]), store.sample(row["assistant_sample"])
                if user["rate"] != self.rate:
                    raise InvalidAudio("听测输入采样率不匹配 / Listening input sample rate mismatch")
                store.read_audio(user, 0, min(user["count"], self.hop), cancel=cancel, verify=True)
                waveform = AudioSequence(int(user["count"]), lambda offset, count: store.read_audio(user, offset, count, cancel=cancel))
                store.read_audio(target, 0, min(target["count"], self.hop), cancel=cancel, verify=True)
                history, history_records = self.review_history(store, row, cancel)
                if "history" in evidence.get("scenarios", []) and not history:
                    raise YuanError("多轮听测缺少真实历史上下文 / Multi-turn listening evaluation lacks real history context")
                output = self.respond_audio(evidence["prompt"], history, waveform, cancel, network=self.network)
                case_id = row["id"]
                input_path, output_path = root / (case_id + "-input.flac"), root / (case_id + "-output.flac")
                for path, values in ((input_path, waveform), (output_path, output)):
                    with sf.SoundFile(str(path), mode="w", samplerate=self.rate, channels=1, format="FLAC", subtype="PCM_24") as output_file:
                        for block in self.audio_blocks(values, cancel):
                            output_file.write(block)
                    with path.open("rb") as stream:
                        os.fsync(stream.fileno())
                cases.append({"history": history_records, "scenarios": evidence.get("scenarios", []), "id": case_id, "pair": row["id"], "language": evidence["language"], "prompt": evidence["prompt"], "recording_id": row["source_key"],
                              "user_digest": user["digest"], "target_digest": target["digest"], "annotation_digest": payload_digest(evidence),
                              "input_samples": user["count"], "input_start": 0, "input_end": user["count"],
                              "input_file": input_path.relative_to(directory).as_posix(), "input_sha256": digest_file(input_path, cancel),
                              "output_file": output_path.relative_to(directory).as_posix(), "output_sha256": digest_file(output_path, cancel)})
            dataset = [{key: case[key] for key in ("id", "pair", "language", "prompt", "recording_id", "user_digest", "target_digest", "annotation_digest", "input_samples", "input_start", "input_end", "scenarios", "history")} for case in cases]
            evaluation = {"protocol": "yuan-native-listening-v3", "evaluator_sha256": behavior_digest(), "build_sha256": digest_file(Path(__file__), cancel), "identity": self.identity(),
                          "model": self.training_manifest, "dataset_sha256": payload_digest(dataset), "cases": cases, "created": time.time(),
                          "acceptance_policy_sha256": acceptance.digest, "minimum_independent_sources": acceptance.minimum}
            evaluation_path = root / "evaluation.json"
            atomic_json(evaluation_path, evaluation, cancel)
            review = {"protocol": evaluation["protocol"], "evaluation_file": evaluation_path.relative_to(directory).as_posix(), "evaluation_sha256": digest_file(evaluation_path, cancel),
                      "reviewer": "", "reviewed_at": None, "checks": [{"id": case["id"], **{key: None for key in ("understandable", "follows_prompt", "relevant_reply", "ends_normally", "no_abnormal_audio")}} for case in cases]}
            atomic_json(root / "review.json", review, cancel)
            old_pointer = read_json(directory / "pending-review.json")
            if old_pointer.get("evaluation_file"):
                self.emit("review_superseded", previous=old_pointer["evaluation_file"], model=self.active_digest)
            atomic_json(directory / "pending-review.json", {"model_sha256": self.active_digest, "state": "pending", "evaluation_file": review["evaluation_file"], "review_file": (root / "review.json").relative_to(directory).as_posix()}, cancel)
            committed = True
            self.emit("notice", key="release_wait", evaluation=evaluation_id, cases=len(cases), model=self.active_digest)
            return True
        finally:
            if not committed:
                shutil.rmtree(root, ignore_errors=True)

    def promote(self, candidate, metrics, cancel, task, store):
        active = copy.deepcopy(candidate).eval().requires_grad_(False)
        if not self._persist(active, metrics, cancel, increment=True, task=task, store=store):
            return False
        self.emit("model", accepted=self.accepted, steps=self.steps, acoustic_ready=self.acoustic_ready, dialogue_trained=self.dialogue_trained, dialogue_ready=self.dialogue_ready,
                  playback_ready=self.playback_ready, conversation_verified=self.dialogue_ready, serving_digest=self.serving_digest,
                  active_digest=self.active_digest, candidate_isolated=self.active_digest != self.serving_digest,
                  capability=self.capability(), identity=self.identity()[:12])
        return True


    def context_budget(self, incoming_count):
        capacity = self.capacity()
        return max(0, min(capacity // 2, capacity - max(self.hop, int(incoming_count))))

    def _history_budget(self, history, incoming_count, recalled=(), context_id=None):
        np = self.np
        budget = self.context_budget(incoming_count)
        def normalize(items):
            selected = []
            total = 0
            for role, audio in items:
                values = np.asarray(audio, dtype=np.float32).reshape(-1)
                total += len(values)
                if role in ("user", "assistant") and values.size and np.isfinite(values).all():
                    selected.append((role, values))
            return selected, total
        recent, recent_total = normalize(history)
        memories, memory_total = normalize(recalled)
        recent_size = sum(len(values) for role, values in recent)
        memory_size = sum(len(values) for role, values in memories)
        if recent_size and memory_size and budget:
            minimum = min(self.hop, budget // 2)
            share = int(budget * math.sqrt(memory_size) / (math.sqrt(memory_size) + math.sqrt(recent_size)))
            memory_budget = min(memory_size, budget - min(recent_size, minimum), max(min(memory_size, minimum), share))
            recent_budget = min(recent_size, budget - memory_budget)
            memory_budget = min(memory_size, budget - recent_budget)
        else:
            memory_budget = min(memory_size, budget)
            recent_budget = min(recent_size, budget - memory_budget)
        def retain(items, limit):
            selected = []
            used = 0
            for role, values in reversed(items):
                take = min(len(values), max(0, limit - used))
                if take:
                    selected.append((role, values[-take:]))
                    used += take
                if used >= limit:
                    break
            return list(reversed(selected)), used
        recent, recent_used = retain(recent, recent_budget)
        memories, memory_used = retain(memories, memory_budget)
        used = recent_used + memory_used
        total = recent_total + memory_total
        if total > used or memory_total:
            self.emit("context_budget", key="context_trimmed" if total > used else "memory_context_retained", id=context_id,
                      kept_samples=used, dropped_samples=total - used, budget_samples=budget, incoming_samples=int(incoming_count),
                      recent_samples=recent_used, recalled_samples=memory_used, recalled_count=len(memories))
        if memory_total:
            self.emit("memory_recall", key="memory_context_retained", id=context_id, count=len(memories), samples=memory_used,
                      matched_samples=memory_total, context_selected=True)
        return tuple(memories + recent)

    @supervised('input_encoding', 'conversation')
    def encode_input(self, prompt, history, waveform, cancel, hidden=None, network=None, final=True, recalled=(), context_id=None):
        np, torch = self.np, self.torch
        incoming = waveform if isinstance(waveform, AudioSequence) else np.asarray(waveform, dtype=np.float32).reshape(-1)
        if not len(incoming) or not isinstance(incoming, AudioSequence) and not np.isfinite(incoming).all():
            raise YuanError("模型输入音频无效 / Invalid model audio input")
        network = network if network is not None else self.serving_network if self.playback_ready and self.serving_network is not None else self.network
        network.eval()
        pending = condition = None
        if isinstance(hidden, AudioEncoding):
            hidden, pending, condition = hidden.hidden, hidden.pending, hidden.condition
        with self.interruptible(cancel), torch.inference_mode():
            if condition is None:
                condition = self._prompt_condition(network, prompt, cancel, "dialogue")
            if hidden is None:
                hidden = self._encode(network, prompt, np.empty(0, dtype=np.float32), history, cancel, task="dialogue", condition=condition,
                                      recalled=recalled, incoming_count=len(incoming), context_id=context_id)
            hidden, pending = self._encode_audio(network, incoming, hidden, "user", cancel, pending, final, condition)
            return hidden.detach() if final else AudioEncoding(hidden.detach(), pending, condition.detach())

    @supervised_stream('generation_block', 'conversation')
    def stream_audio(self, prompt, history, waveform, cancel, max_samples=None, network=None, hidden=None, reference_rms=None):
        np, torch = self.np, self.torch
        incoming = waveform if isinstance(waveform, AudioSequence) else np.asarray(waveform, dtype=np.float32).reshape(-1)
        if not len(incoming) or not isinstance(incoming, AudioSequence) and not np.isfinite(incoming).all():
            raise YuanError("模型输入音频无效 / Invalid model audio input")
        if reference_rms is None:
            energy = 0.0
            for values in self.audio_blocks(incoming, cancel):
                values = values.astype(np.float64)
                energy += float(np.dot(values, values))
            reference = math.sqrt(energy / len(incoming))
        else:
            reference = float(reference_rms)
        if not math.isfinite(reference) or reference <= np.finfo(np.float32).eps:
            raise YuanError("模型输入为静音 / Model input is silent")
        network = network if network is not None else self.serving_network if self.playback_ready and self.serving_network is not None else self.network
        count = max(self.hop, min(int(max_samples) if max_samples is not None else self.reply_samples(incoming, history), self.capacity()))
        block_seconds = self.policy.number("inference", "stream_block_seconds", max(self.hop / self.rate, math.sqrt(self.hop / self.rate)), self.hop / self.rate, max(self.hop / self.rate, self.chunk_seconds))
        block_frames = max(1, min(math.ceil(count / self.hop), int(round(block_seconds * self.rate / self.hop))))
        self.emit("phase", key="prefill", current=len(incoming), total=self.capacity(), unit="samples", reply_samples=count, reply_seconds=count / self.rate, streaming=True)
        network.eval()
        if hidden is None:
            hidden = self.encode_input(prompt, history, incoming, cancel, network=network)
        if isinstance(hidden, AudioEncoding):
            with self.interruptible(cancel), torch.inference_mode():
                hidden, pending = self._encode_audio(network, np.empty(0, dtype=np.float32), hidden.hidden, "user", cancel, hidden.pending, True, hidden.condition)
        condition = hidden
        previous = network.begin
        total_frames = max(1, math.ceil(count / self.hop))
        emitted = frames_done = 0
        compute_seconds = energy = 0.0
        energy_samples = 0
        gain = None
        natural = False
        try:
            while frames_done < total_frames and not natural:
                began = time.monotonic()
                pieces = []
                with self.interruptible(cancel), torch.inference_mode():
                    for _ in range(min(block_frames, total_frames - frames_done)):
                        check(cancel)
                        predicted, ending, hidden = network.decode(previous, hidden, condition)
                        if not torch.isfinite(predicted).all() or not torch.isfinite(ending).all():
                            raise YuanError("模型输出含无效数值 / Model output contains invalid values")
                        pieces.append(predicted)
                        previous = predicted
                        frames_done += 1
                        if ending.item() > 0:
                            natural = True
                            break
                    values = torch.cat(pieces, dim=1).reshape(-1).detach().float().cpu().numpy()[:count - emitted].copy()
                compute_seconds += max(tick(), time.monotonic() - began)
                energy += float(np.dot(values.astype(np.float64), values.astype(np.float64)))
                energy_samples += len(values)
                rms = math.sqrt(energy / max(1, energy_samples))
                next_gain = min(1.0, reference / max(np.finfo(np.float32).eps, rms))
                if gain is None:
                    gain = next_gain
                values *= np.linspace(gain, next_gain, len(values), dtype=np.float32)
                gain = next_gain
                emitted += len(values)
                check(cancel)
                self.emit("phase", key="generating", current=frames_done, total=total_frames, unit="frames", streaming=True)
                yield np.ascontiguousarray(values, dtype=np.float32)
            check(cancel)
            if energy_samples == 0 or energy <= np.finfo(np.float32).eps ** 2 * energy_samples:
                raise YuanError("模型输出为静音 / Model output is silent")
            self.emit("generation_end", key="natural_end" if natural else "limited", natural=natural, limited=not natural, frames=frames_done, samples=emitted, streaming=True)
            if not natural:
                self.emit("notice", key="limited", component="generation")
        finally:
            if emitted and compute_seconds > 0:
                section = getattr(self, "performance_section", "performance")
                self.policy.observe(section, "generation_realtime_factor", compute_seconds * self.rate / emitted)
                self.emit("generation_measurement", compute_seconds=compute_seconds, samples=emitted, realtime_factor=compute_seconds * self.rate / emitted,
                          device=str(self.device), cancelled=bool(cancel and cancel.is_set()))

    def calibrate(self, cancel, force=False):
        import psutil
        torch = self.torch
        try:
            available = len(psutil.Process().cpu_affinity())
        except (AttributeError, OSError, psutil.Error):
            available = os.cpu_count() or 1
        section = getattr(self, "performance_section", "performance")
        upper = self.policy.integer("execution", "calibration_thread_limit", max(1, math.isqrt(available)), 1, max(1, available))
        chosen = int(self.policy.measured(section, "best_threads", 0))
        if not force and 1 <= chosen <= upper:
            torch.set_num_threads(chosen)
            return
        previous = torch.get_num_threads()
        best = None
        count = self.hop * max(4, self.layers + 1)
        values = self.np.linspace(-0.1, 0.1, count, dtype=self.np.float32)
        candidates = sorted({1, min(previous, upper), upper})
        try:
            for threads in candidates:
                check(cancel)
                torch.set_num_threads(threads)
                for trial in range(2):
                    began = time.monotonic()
                    with torch.inference_mode():
                        hidden = self._encode(self.network, "Yuan", values, cancel=cancel, task="dialogue")
                        output = self._decode(self.network, hidden, count, cancel, stop=False)
                        output.float().cpu().numpy()
                    elapsed = max(tick(), time.monotonic() - began)
                    if trial:
                        self.emit("calibration", threads=threads, seconds=elapsed, samples=count, device=str(self.device))
                        if best is None or elapsed < best[0]:
                            best = (elapsed, threads)
            chosen = best[1] if best else previous
            torch.set_num_threads(chosen)
            self.policy.observe(section, "best_threads", chosen)
            self.policy.save()
        except BaseException:
            torch.set_num_threads(previous)
            raise


class AudioLearner:
    def __init__(self, model, store, emit):
        self.model, self.store, self.emit = model, store, emit
        self.steps = model.steps
        self.data_revision = store.setting("data_revision")
        self.candidate = None
        self.optimizer = None
        self.training_loss = None
        self.dirty = False
        self.checkpoint_base = model.active_digest
        frames = max(1, model.capacity() // model.hop)
        self.validation_limit = model.policy.integer("learning", "validation_reservoir", max(1, math.isqrt(frames)), 1, max(1, math.isqrt(frames) * max(1, model.layers)))
        interval_default = max(1, math.isqrt(max(1, model.width * model.layers)))
        self.validation_interval = model.policy.integer("learning", "validation_interval", interval_default, 1, max(interval_default, math.isqrt(frames)))
        self.checkpoint_interval = model.policy.integer("learning", "checkpoint_interval", max(self.validation_interval, interval_default * 2), 1, max(self.validation_interval * 8, math.isqrt(frames) * 2))
        self.dialogue_mix = model.policy.ratio("learning", "dialogue_mix", 1 / math.sqrt(2))
        self.last_validation_step = self.steps
        self.last_checkpoint_step = self.steps
        self.training_seconds = None
        self.validation_seconds = None
        self.checkpoint_seconds = None
        self.model.policy.save()

    def _state(self):
        return {"steps": self.steps, "accepted": self.model.accepted, "acoustic_ready": self.model.acoustic_ready,
                "learning_model": self.model.active_digest, "serving_model": self.model.serving_digest,
                "candidate_isolated": bool(self.model.active_digest and self.model.active_digest != self.model.serving_digest),
                "dialogue_ready": self.model.dialogue_ready, "dialogue_trained": self.model.dialogue_trained, "playback_ready": self.model.playback_ready, "capability": self.model.capability()}

    def _resume_activation(self, cancel):
        if not self.model._resume_training_activation(self.store, cancel):
            return False
        self.steps = self.model.steps
        self._discard_candidate()
        return True

    def _refresh_data(self):
        revision = self.store.setting("data_revision")
        if revision == self.data_revision:
            return False
        self._discard_candidate()
        self.data_revision = revision
        self.training_loss = None
        self.last_validation_step = self.steps
        self.emit("learning", key="annotations_changed", data_revision=revision, **self._state())
        return True

    def _optimizer(self, network):
        parameters = sum(value.numel() for value in network.parameters())
        return self.model.torch.optim.AdamW(network.parameters(), lr=1 / math.sqrt(parameters), weight_decay=1 / parameters)

    @supervised('candidate_prepare', 'learning')
    def _prepare(self, cancel):
        if self.candidate is not None:
            return
        torch = self.model.torch
        check(cancel)
        self.candidate = self.model.clone().train().requires_grad_(True)
        self.optimizer = self._optimizer(self.candidate)
        directory = safe_path(self.model.state, "learning")
        manifest = read_json(directory / "candidate.json")
        if not isinstance(manifest, dict) or manifest.get("identity") != self.model.identity() or manifest.get("data_revision") != self.data_revision:
            return
        try:
            target = safe_path(directory, manifest["file"])
            if manifest.get("base") != self.model.active_digest or digest_file(target, cancel) != manifest.get("sha256"):
                raise YuanError("候选存档基线或摘要不匹配 / Candidate checkpoint baseline or digest mismatch")
            state = torch.load(str(target), map_location="cpu", weights_only=True)
            if not all(isinstance(value, torch.Tensor) and torch.isfinite(value).all() for value in state["network"].values()):
                raise YuanError("候选权重无效 / Invalid candidate weights")
            self.candidate.load_state_dict(state["network"], strict=True)
            self.optimizer.load_state_dict(state["optimizer"])
            for values in self.optimizer.state.values():
                for value in values.values():
                    if isinstance(value, torch.Tensor) and not torch.isfinite(value).all():
                        raise YuanError("优化器状态无效 / Invalid optimizer state")
            steps = int(state["steps"])
            training_loss = state.get("training_loss")
            if steps < 0 or training_loss is not None and not math.isfinite(float(training_loss)):
                raise YuanError("候选训练指标无效 / Invalid candidate training measurements")
            self.steps = max(self.model.steps, steps)
            self.model.steps = self.steps
            self.training_loss = float(training_loss) if training_loss is not None else None
            self.last_validation_step = min(self.steps, max(self.model.steps, int(state.get("last_validation_step", self.last_validation_step))))
            self.last_checkpoint_step = self.steps
            self.training_seconds = state.get("training_seconds") if isinstance(state.get("training_seconds"), (int, float)) else self.training_seconds
            self.validation_seconds = state.get("validation_seconds") if isinstance(state.get("validation_seconds"), (int, float)) else self.validation_seconds
            self.dirty = False
            self.checkpoint_base = self.model.active_digest
            self.emit("learning", key="candidate_recovered", **self._state())
        except Cancelled:
            self.candidate = self.optimizer = None
            raise
        except Exception as exc:
            self.emit("notice", key="checkpoint_failure", component="candidate", reason=concise_error(exc))
            self.candidate = self.model.clone().train().requires_grad_(True)
            self.optimizer = self._optimizer(self.candidate)
            self.steps = self.model.steps
            self.training_loss = None

    @supervised('checkpointing', 'learning')
    def checkpoint(self, cancel, force=False):
        self._refresh_data()
        if self.candidate is None or not self.dirty and self.checkpoint_base == self.model.active_digest:
            return
        if not force and self.steps - self.last_checkpoint_step < self.checkpoint_interval:
            return
        began = time.monotonic()
        self.emit("phase", key="checkpointing", steps=self.steps)
        directory = safe_path(self.model.state, "learning")
        name = "candidate-" + uuid.uuid4().hex + ".pt"
        target = directory / name
        temporary = target.with_suffix(".partial")
        committed = False
        with StorageBudget(self.model.state).reserve(max(1, self.model.model_bytes * 4), "checkpoint"):
            try:
                check(cancel)
                payload = {"network": {key: value.detach().cpu().contiguous() for key, value in self.candidate.state_dict().items()},
                           "optimizer": self.optimizer.state_dict(), "steps": self.steps, "training_loss": self.training_loss,
                           "last_validation_step": self.last_validation_step, "training_seconds": self.training_seconds, "validation_seconds": self.validation_seconds}
                self.model.torch.save(payload, str(temporary))
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                check(cancel)
                os.replace(temporary, target)
                atomic_json(directory / "candidate.json", {"identity": self.model.identity(), "base": self.model.active_digest,
                            "file": name, "sha256": digest_file(target, cancel), "steps": self.steps, "data_revision": self.data_revision, "created": time.time()}, cancel)
                committed = True
                self.dirty = False
                self.checkpoint_base = self.model.active_digest
                self.last_checkpoint_step = self.steps
                measured = max(tick(), time.monotonic() - began)
                self.checkpoint_seconds = measured if self.checkpoint_seconds is None else math.sqrt(self.checkpoint_seconds * measured)
                if self.training_seconds:
                    suggested = max(1, int(math.ceil(self.checkpoint_seconds / max(tick(), self.training_seconds))))
                    self.checkpoint_interval = max(self.validation_interval, int(round(math.sqrt(self.checkpoint_interval * max(self.validation_interval, suggested)))))
                self.emit("checkpoint_saved", steps=self.steps, base=self.checkpoint_base)
            finally:
                temporary.unlink(missing_ok=True)
                if not committed:
                    target.unlink(missing_ok=True)
            for old in directory.glob("candidate-*.pt"):
                if old != target:
                    try:
                        old.unlink(missing_ok=True)
                    except OSError as exc:
                        self.emit("notice", reason=concise_error(exc), component="candidate_cleanup")

    def _discard_candidate(self):
        directory = safe_path(self.model.state, "learning")
        manifest = read_json(directory / "candidate.json")
        if isinstance(manifest, dict) and isinstance(manifest.get("file"), str):
            try:
                safe_path(directory, manifest["file"]).unlink(missing_ok=True)
            except (OSError, ValueError):
                pass
        (directory / "candidate.json").unlink(missing_ok=True)
        for old in directory.glob("candidate-*.pt"):
            old.unlink(missing_ok=True)
        self.candidate = self.optimizer = None
        self.dirty = False
        self.checkpoint_base = self.model.active_digest
        self.last_checkpoint_step = self.steps

    def _prompt(self, group_id):
        session = self.store.db.execute("SELECT prompt FROM sessions WHERE id=?", (group_id,)).fetchone()
        if session and isinstance(session["prompt"], str) and session["prompt"].strip():
            return session["prompt"]
        source_id = group_id.removeprefix("public-")
        source = self.store.db.execute("SELECT metadata FROM sources WHERE id=?", (source_id,)).fetchone()
        language = self.store.setting("language", "中文")
        if source:
            try:
                metadata = json.loads(source["metadata"])
                if metadata.get("language") == "English":
                    language = "English"
                elif metadata.get("language") == "Mixed":
                    return tr("default_prompt", "中文") + " / " + tr("default_prompt", "English")
                else:
                    language = "中文"
            except (ValueError, TypeError):
                pass
        return tr("default_prompt", language)

    def _audio(self, row, cancel, start, count):
        check(cancel)
        values = self.store.read_audio(row, start, count, cancel=cancel)
        if row["rate"] != self.model.rate:
            raise YuanError("历史音频采样率不匹配 / Historical audio sample rate mismatch")
        check(cancel)
        return values

    def _context(self, group_id, before_created, source_count, cancel, excluded=()):
        hop = self.model.hop
        window = self.model.window()
        budget = max(0, self.model.capacity() // 2)
        used = 0
        context = []
        rows = self.store.db.execute("SELECT * FROM samples WHERE group_id=? AND created<? AND audio_status='ready' AND memory_eligible=1 ORDER BY created DESC", (group_id, before_created))
        for previous in rows:
            check(cancel)
            if previous["id"] in excluded:
                continue
            remaining = budget - used
            if remaining < hop:
                break
            take = min(window, previous["count"], remaining)
            if take < hop:
                continue
            try:
                values = self._audio(previous, cancel, previous["count"] - take, take)
                role = "assistant" if previous["role"] == "assistant" else "user"
                context.append((role, values))
                used += len(values)
                self.emit("learning_context", key="history_context", sample=previous["id"], generated_as_target=False)
            except (OSError, RuntimeError, ValueError, YuanError) as exc:
                self.emit("notice", key="sample_skipped", sample=previous["id"], reason=concise_error(exc))
            if used >= budget:
                break
        context.reverse()
        return tuple(context)

    def endpoint(self, row, end):
        metadata = json.loads(row["metadata"])
        final = metadata.get("utterance_final")
        valid = isinstance(final, bool) and not any(metadata.get(key) for key in ("capture_gap", "playback_overlap", "interrupted", "discontinuous", "final"))
        if metadata.get("limited") and final is not False:
            valid = False
        return bool(valid and final and end == row["count"]), bool(valid)

    def acoustic_features(self, row, cancel, held_out=False):
        hop = self.model.hop
        total = row["count"]
        if row["generated"] or not row["learnable"] or row["task"] != "acoustic" or total < hop * 2:
            raise IneligibleSample("样本不能作为真实声学目标 / Sample cannot be used as a real acoustic target")
        window = self.model.window()
        if held_out:
            slots = max(1, (total - hop * 2) // hop + 1)
            cursor = (int(row["digest"][:16], 16) % slots) * hop
        else:
            last_start = ((total - hop) // hop) * hop
            cursor = max(0, min(int(row["cursor"]), last_start))
        start = max(hop, cursor)
        start -= start % hop
        end = min(total, start + window)
        source_start = max(0, start - window)
        audio = self._audio(row, cancel, source_start, end - source_start)
        cut = start - source_start
        source, target = audio[:cut], audio[cut:]
        if len(target) < hop:
            raise IneligibleSample("音频目标过短 / Audio target is too short")
        context = self._context(row["group_id"], row["created"], len(source), cancel, (row["id"],)) if row["origin"] == "local" else ()
        ended, end_label_valid = self.endpoint(row, end)
        return {"prompt": self._prompt(row["group_id"]), "source": source, "target": target, "ended": ended, "end_label_valid": end_label_valid, "context": context, "task": "acoustic"}, end

    def dialogue_features(self, pair, cancel, held_out=False, window_start=None):
        hop = self.model.hop
        if not pair["verified"]:
            raise IneligibleSample("对话配对未经验证 / Dialogue pair is not verified")
        user = self.store.sample(pair["user_sample"])
        target_row = self.store.sample(pair["assistant_sample"])
        if user is None or target_row is None or user["generated"] or target_row["generated"] or not user["dialogue_eligible"] or not target_row["dialogue_eligible"]:
            raise IneligibleSample("对话配对不能使用生成音频作为目标 / Generated audio cannot be a dialogue target")
        total = target_row["count"]
        if user["count"] < hop or total < hop:
            raise IneligibleSample("对话配对音频过短 / Dialogue pair audio is too short")
        window = max(hop, self.model.window() // hop * hop)
        if window_start is not None:
            if isinstance(window_start, bool) or not isinstance(window_start, int) or window_start < 0 or window_start % hop or window_start > total - hop:
                raise IneligibleSample("验证窗口位置无效 / Invalid validation window position")
            cursor = window_start
        elif held_out:
            cursor = 0
        else:
            cursor = max(0, min(int(pair["cursor"]), max(0, total - hop)))
        start = cursor - cursor % hop
        end = min(total, start + window)
        source = AudioSequence(int(user["count"]), lambda offset, count: self._audio(user, cancel, offset, count))
        target = self._audio(target_row, cancel, start, end - start)
        if len(target) < hop:
            raise IneligibleSample("对话目标过短 / Dialogue target is too short")
        context = self._context(pair["group_id"], user["created"], len(source), cancel, (user["id"], target_row["id"]))
        def prefix():
            for offset in range(0, start, window):
                yield self._audio(target_row, cancel, offset, min(window, start - offset))
        evidence = json.loads(pair["evidence"])
        ended, end_label_valid = self.endpoint(target_row, end)
        return {"prompt": evidence["prompt"], "source": source, "target": target, "ended": ended, "end_label_valid": end_label_valid, "context": context, "task": "dialogue", "prefix": prefix if start else None}, end, target_row["count"]

    def _usable(self, row, task, cancel, held_out=False, window_start=None):
        try:
            usable = self.dialogue_features(row, cancel, held_out, window_start) if task == "dialogue" else self.acoustic_features(row, cancel, held_out)
            if task == "dialogue" and row["failures"]:
                self.store.db.execute("UPDATE turn_pairs SET retry_at=0,failures=0,error='' WHERE id=?", (row["id"],))
                self.store.commit()
            return usable
        except IneligibleSample as exc:
            self.store.exclude_task(row, task, concise_error(exc))
            self.emit("notice", key="sample_skipped", sample=row["id"], task=task, retryable=False, reason=concise_error(exc))
            return None
        except Cancelled:
            raise
        except (OSError, RuntimeError, ValueError) as exc:
            if task == "dialogue":
                self.store.pair_failure(row, exc)
            else:
                self.store.audio_failure(row, exc)
            self.emit("notice", key="sample_skipped", sample=row["id"], task=task, retryable=not isinstance(exc, InvalidAudio), reason=concise_error(exc))
            return None

    def _held_out(self, task, split):
        if task != "dialogue":
            return self.store.held_out(split, self.validation_limit, min_count=self.model.hop * 2)
        return AcceptancePolicy(self.model).select(self.store, self.model, split, require_controls=True)

    def _evaluation_starts(self, row, task):
        if task != "dialogue":
            return (None,)
        target = self.store.sample(row["assistant_sample"])
        if target is None:
            return (0,)
        hop = self.model.hop
        window = max(hop * 2, self.model.window() // hop * hop)
        last = max(0, math.ceil((target["count"] - window) / hop))
        if not last:
            return (0,)
        count = self.model.policy.integer("learning", "validation_windows", min(self.model.layers + 2, last + 1), 2, last + 1)
        return tuple(sorted({round(index * last / (count - 1)) * hop for index in range(count)}))

    def _evaluate(self, rows, task, cancel):
        torch = self.model.torch
        before, after, weights = {}, {}, {}
        self.candidate.eval()
        for index, row in enumerate(rows):
            check(cancel)
            starts = self._evaluation_starts(row, task)
            base_total = candidate_total = weight = 0.0
            for position, start in enumerate(starts):
                self.emit("phase", key="validating", current=index, total=len(rows), unit="groups", split=row["split"], objective=task,
                          window=position + 1, windows=len(starts), window_start=start)
                with operation(self.emit, "validating", "learning"), self.model.interruptible(cancel), torch.no_grad():
                    usable = self._usable(row, task, cancel, held_out=True, window_start=start)
                    if usable is None:
                        return None
                    features = usable[0]
                    base = float(self.model.loss(self.model.network, **features, rollout=True).item())
                    candidate = float(self.model.loss(self.candidate, **features, rollout=True).item())
                if not math.isfinite(base) or not math.isfinite(candidate):
                    raise YuanError("验证指标无效 / Validation measurements are not finite")
                samples = len(features["target"])
                base_total += base * samples
                candidate_total += candidate * samples
                weight += samples
                self.emit("validation_window", sample=row["id"], group=row["group_id"], split=row["split"], objective=task,
                          start=start, end=usable[1], samples=samples, before=base, after=candidate, ending=features["ended"])
            group = row["group_id"]
            before[group] = before.get(group, 0.0) + base_total
            after[group] = after.get(group, 0.0) + candidate_total
            weights[group] = weights.get(group, 0.0) + weight
            self.emit("phase", key="validating", current=index + 1, total=len(rows), unit="groups", split=row["split"], objective=task)
        return {group: total / weights[group] for group, total in before.items()}, {group: total / weights[group] for group, total in after.items()}

    def _comparison(self, before, after, require_improvement=True):
        keys = tuple(before)
        improvements = [before[key] - after[key] for key in keys]
        baseline = sum(before.values()) / len(before)
        updated = sum(after.values()) / len(after)
        mean = sum(improvements) / len(improvements)
        variance = sum((value - mean) ** 2 for value in improvements) / max(1, len(improvements))
        scale = max(1.0, abs(baseline))
        margin = max(math.sqrt(self.model.torch.finfo(self.model.dtype).eps) * scale, math.sqrt(variance / max(1, len(improvements))))
        worst = min(improvements)
        if require_improvement:
            passed = mean > margin and worst >= -margin * math.sqrt(max(1, len(improvements)))
        else:
            passed = updated <= baseline + margin and worst >= -margin * math.sqrt(max(1, len(improvements)))
        return passed, baseline, updated, margin

    def _contrast_features(self, pair, features, dimension, control, cancel):
        alternative = dict(features)
        if dimension == "prompt":
            alternative["prompt"] = control["prompt"]
        elif dimension == "input":
            row = self.store.sample(control["sample"])
            if row is None or row["group_id"] != pair["group_id"] or row["generated"] or not row["dialogue_eligible"] or row["role"] != "user" or row["id"] == pair["user_sample"]:
                raise IneligibleSample("输入对照来源不符合独立验收要求 / Input contrast provenance does not satisfy independent acceptance")
            self.store.read_audio(row, 0, min(row["count"], self.model.hop), cancel=cancel, verify=True)
            alternative["source"] = AudioSequence(row["count"], lambda offset, count: self._audio(row, cancel, offset, count))
        else:
            context = []
            for identifier in control["samples"]:
                row = self.store.sample(identifier)
                if row is None or row["group_id"] != pair["group_id"] or row["generated"] or not row["dialogue_eligible"] or row["id"] in (pair["user_sample"], pair["assistant_sample"]):
                    raise IneligibleSample("历史对照来源不符合独立验收要求 / History contrast provenance does not satisfy independent acceptance")
                take = min(row["count"], self.model.window())
                context.append((row["role"], self._audio(row, cancel, row["count"] - take, take)))
            if not features["context"] and not context:
                raise IneligibleSample("历史对照没有改变上下文 / History contrast does not change the context")
            alternative["context"] = tuple(context)
        return alternative

    def _regression(self, rows, cancel):
        from statistics import NormalDist
        acceptance = AcceptancePolicy(self.model)
        observed = {}
        evidence_rows = []
        self.candidate.eval()
        for pair in rows:
            check(cancel)
            evidence = json.loads(pair["evidence"])
            controls = evidence.get("contrasts", {})
            if not controls:
                continue
            usable = self._usable(pair, "dialogue", cancel, held_out=True)
            if usable is None:
                continue
            features = usable[0]
            with self.model.interruptible(cancel), self.model.torch.no_grad():
                base = float(self.model.loss(self.model.network, **features, rollout=True).item())
                candidate = float(self.model.loss(self.candidate, **features, rollout=True).item())
                for dimension, control in controls.items():
                    check(cancel)
                    alternative = self._contrast_features(pair, features, dimension, control, cancel)
                    wrong_base = float(self.model.loss(self.model.network, **alternative, rollout=True).item())
                    wrong_candidate = float(self.model.loss(self.candidate, **alternative, rollout=True).item())
                    if not all(math.isfinite(value) for value in (base, candidate, wrong_base, wrong_candidate)):
                        raise YuanError("对照验收产生无效测量 / Contrast acceptance produced invalid measurements")
                    key = pair["split"] + ":" + evidence["language"] + ":" + dimension
                    observed.setdefault(key, {}).setdefault(pair["source_key"], []).append((wrong_base - base, wrong_candidate - candidate))
            evidence_rows.append({"pair": pair["id"], "source": pair["source_key"], "split": pair["split"], "annotation_sha256": payload_digest(evidence)})
        checks, missing = {}, []
        z = NormalDist().inv_cdf((1 + acceptance.confidence) / 2)
        for split in ("validation", "guard"):
            for language in acceptance.languages:
                for dimension in acceptance.dimensions:
                    key = split + ":" + language + ":" + dimension
                    values = [(sum(item[0] for item in source) / len(source), sum(item[1] for item in source) / len(source))
                              for source in observed.get(key, {}).values()]
                    if len(values) < acceptance.minimum:
                        missing.append(key)
                        continue
                    margins = [after for before, after in values]
                    changes = [after - before for before, after in values]
                    def bound(numbers):
                        mean = sum(numbers) / len(numbers)
                        variance = sum((value - mean) ** 2 for value in numbers) / max(1, len(numbers) - 1)
                        return mean, z * math.sqrt(variance / len(numbers))
                    margin, uncertainty = bound(margins)
                    change, change_uncertainty = bound(changes)
                    tolerance = math.sqrt(self.model.torch.finfo(self.model.dtype).eps) * max(1.0, abs(margin))
                    passed = margin - uncertainty > tolerance and change - change_uncertainty >= -tolerance
                    checks[key] = {"sources": len(values), "margin": margin, "uncertainty": uncertainty, "margin_change": change,
                                   "change_uncertainty": change_uncertainty, "passed": passed}
        return {"protocol": "yuan-annotated-contrast-v1", "acceptance_policy_sha256": acceptance.digest, "checks": checks,
                "missing": missing, "passed": bool(checks) and not missing and all(row["passed"] for row in checks.values()),
                "evidence_sha256": payload_digest(evidence_rows), "semantic_acceptance": False}

    def _select(self):
        pair = self.store.next_pair("train", self.model.hop, self.model.hop)
        sample = self.store.next_sample("train", self.model.hop * 2)
        if pair is None:
            return ("acoustic", sample) if sample is not None else (None, None)
        if sample is None:
            return "dialogue", pair
        phase = (self.steps * ((math.sqrt(5) - 1) / 2)) % 1
        dialogue_threshold = self.dialogue_mix if self.model.dialogue_ready else self.dialogue_mix + (1 - self.dialogue_mix) / 2
        if phase < dialogue_threshold:
            return "dialogue", pair
        return "acoustic", sample

    def _validate(self, task, train_row, cancel):
        began = time.monotonic()
        rows = {split: self._held_out(task, split) for split in ("validation", "guard")}
        if any(not values for values in rows.values()):
            self.emit("learning", key="training_unvalidated", train=self.training_loss, objective=task,
                      validation_groups=len(rows["validation"]), guard_groups=len(rows["guard"]), **self._state())
            return False
        validation_groups = {sample["group_id"] for sample in rows["validation"]}
        guard_groups = {sample["group_id"] for sample in rows["guard"]}
        if train_row["group_id"] in validation_groups | guard_groups or validation_groups & guard_groups:
            raise YuanError("训练、验证与保护数据来源重叠 / Training, validation and guard sources overlap")
        self.store.assert_independent(list({train_row["group_id"]} | validation_groups | guard_groups))
        self.emit("learning", key="validating", objective=task, **self._state())
        comparisons = {split: self._evaluate(values, task, cancel) for split, values in rows.items()}
        if any(value is None for value in comparisons.values()):
            self.emit("learning", key="training_unvalidated", objective=task, **self._state())
            return False
        metrics = {"train": self.training_loss, "groups": {}, "objective": "dialogue reply from real paired audio" if task == "dialogue" else "native acoustic continuation",
                   "conversation_verified": False, "data_revision": self.data_revision}
        passed = True
        for split, (before, after) in comparisons.items():
            okay, baseline, updated, margin = self._comparison(before, after, True)
            passed = passed and okay
            metrics["before" if split == "validation" else "guard_before"] = baseline
            metrics["after" if split == "validation" else "guard_after"] = updated
            metrics[split + "_margin"] = margin
            metrics["groups"][split] = {key: {"before": before[key], "after": after[key]} for key in before}
        preserve_task = "dialogue" if task == "acoustic" and self.model.dialogue_trained else "acoustic" if task == "dialogue" else None
        if preserve_task is not None:
            preservation = {split: self._held_out(preserve_task, split) for split in ("validation", "guard")}
            if any(not values for values in preservation.values()):
                passed = False
            else:
                preserve_checks = {split: self._evaluate(values, preserve_task, cancel) for split, values in preservation.items()}
                if any(value is None for value in preserve_checks.values()):
                    passed = False
                else:
                    for split, (before, after) in preserve_checks.items():
                        okay, baseline, updated, margin = self._comparison(before, after, False)
                        passed = passed and okay
                        metrics[preserve_task + "_" + split] = {"before": baseline, "after": updated, "margin": margin}
        if task == "dialogue" or self.model.dialogue_trained:
            regression_rows = [value for split in ("validation", "guard") for value in self._held_out("dialogue", split)]
            regression = self._regression(regression_rows, cancel)
            metrics["regression"] = regression
            passed = passed and regression["passed"]
        evidence = []
        for record in [train_row, *[value for values in rows.values() for value in values]]:
            evidence.append({"group": record["group_id"], "sample": record["id"], "split": record["split"],
                             "content": payload_digest(dict(record))})
        metrics["data_evidence_sha256"] = payload_digest(evidence)
        metrics["training_validated"] = bool(passed)
        metrics["conversation_verified"] = False
        metrics["serving_conversation_verified"] = self.model.dialogue_ready
        measured = max(tick(), time.monotonic() - began)
        self.validation_seconds = measured if self.validation_seconds is None else math.sqrt(self.validation_seconds * measured)
        if self.training_seconds:
            suggested = max(1, int(math.ceil(self.validation_seconds / max(tick(), self.training_seconds))))
            self.validation_interval = max(1, int(round(math.sqrt(self.validation_interval * suggested))))
        self.last_validation_step = self.steps
        check(cancel)
        if self._refresh_data():
            return False
        if passed:
            passed = self.model.promote(self.candidate, metrics, cancel, task, self.store)
            if not passed:
                self._refresh_data()
                return False
            self._discard_candidate()
        self.emit("learning", key="promoted" if passed else "kept", **self._state(), **metrics)
        return passed

    def step(self, cancel):
        self._resume_activation(cancel)
        self._refresh_data()
        torch = self.model.torch
        task, row = self._select()
        if row is None:
            self.emit("learning", key="waiting_data", **self._state())
            return False
        with operation(self.emit, "training_data", "learning"):
            usable = self._usable(row, task, cancel)
        if usable is None:
            return False
        features = usable[0]
        cursor = usable[1]
        target_count = usable[2] if task == "dialogue" else None
        self._prepare(cancel)
        check(cancel)
        self.emit("learning", key="training_dialogue" if task == "dialogue" else "training_acoustic", sample=row["id"], group=row["group_id"], origin=row["origin"], split=row["split"], objective=task, context=len(features["context"]), **self._state())
        self.candidate.train().requires_grad_(True)
        began = time.monotonic()
        allocated = None
        measured_frames = max(self.model.width, math.ceil(self.model.window() / self.model.hop))
        if self.model.device.type == "cuda":
            try:
                torch.cuda.reset_peak_memory_stats(self.model.device)
                allocated = torch.cuda.memory_allocated(self.model.device)
            except RuntimeError:
                pass
        try:
            with operation(self.emit, "training_compute", "learning"), self.model.interruptible(cancel):
                self.optimizer.zero_grad(set_to_none=True)
                loss = self.model.loss(self.candidate, **features)
                if not torch.isfinite(loss):
                    self.candidate = self.optimizer = None
                    self.dirty = False
                    self.emit("notice", key="candidate_reset")
                    raise YuanError("学习损失无效 / Learning loss is not finite")
                loss.backward()
                check(cancel)
                parameters = sum(value.numel() for value in self.candidate.parameters())
                torch.nn.utils.clip_grad_norm_(self.candidate.parameters(), math.sqrt(parameters), error_if_nonfinite=True)
                self.optimizer.step()
                if any(not torch.isfinite(value).all() for value in self.candidate.parameters()):
                    self.candidate = self.optimizer = None
                    self.dirty = False
                    self.emit("notice", key="candidate_reset")
                    raise YuanError("更新产生无效数值 / Update produced non-finite values")
                self.training_loss = float(loss.item())
                self.steps += 1
                self.model.steps = self.steps
                self.dirty = True
            if allocated is not None:
                peak = max(0, torch.cuda.max_memory_allocated(self.model.device) - allocated)
                self.model.policy.observe(self.model.performance_section, "training_bytes_per_frame", peak / measured_frames)
                self.emit("learning_memory", device=str(self.model.device), peak_increment_bytes=peak, frames=measured_frames)
            self.model.training_retry_at = 0.0
            self.model.training_oom_window = None
            measured = max(tick(), time.monotonic() - began)
            self.training_seconds = measured if self.training_seconds is None else math.sqrt(self.training_seconds * measured)
            if self._refresh_data():
                return False
            if task == "dialogue":
                self.store.advance_pair(row, cursor, target_count)
            else:
                self.store.advance(row, cursor)
            if self.steps - self.last_validation_step >= self.validation_interval:
                self._validate(task, row, cancel)
            self.checkpoint(cancel)
            return True
        finally:
            if self.optimizer is not None:
                self.optimizer.zero_grad(set_to_none=True)


class DatasetImporter:
    def __init__(self, state, store, model, emit, namespace="workspace"):
        if not isinstance(namespace, str) or not namespace.strip():
            raise YuanError("数据集来源命名空间无效 / Invalid dataset source namespace")
        self.namespace = namespace
        self.root = safe_path(state, "datasets")
        self.root.mkdir(parents=True, exist_ok=True)
        self.store, self.model, self.emit = store, model, emit
        self.failures = {}

    def identity_key(self, path, data):
        identifier = data.get("dataset_id", path.name)
        if not isinstance(identifier, str) or not identifier.strip():
            raise YuanError("数据集身份无效 / Invalid dataset identity")
        return "dataset_" + payload_digest([self.namespace, identifier])

    @supervised('dataset_import', 'learning')
    def step(self, cancel):
        with self.store.audio_files(cancel):
            return self._step(cancel)

    def _step(self, cancel):
        import numpy as np
        import soundfile as sf
        import psutil
        for path in sorted(self.root.glob("*.json")):
            check(cancel)
            if path.is_symlink():
                continue
            staged = []
            published = False
            checksum = None
            try:
                checksum = digest_file(path, cancel)
                failure = self.failures.get(path.name)
                if failure and failure["checksum"] == checksum and time.monotonic() < failure["retry_at"]:
                    continue
                data = checked_json(path)
                key = self.identity_key(path, data)
                file_key = "dataset_file_" + payload_digest([self.namespace, path.name])
                if self.store.setting(file_key) not in (None, key):
                    raise YuanError("同一数据集文件不能更换数据集身份 / A dataset file cannot change its dataset identity")
                if self.store.setting(key) == checksum:
                    self.store.set_setting(file_key, key)
                    continue
                required = ("recording_id", "reviewer", "annotation_id", "prompt", "license")
                if data.get("format") != "yuan-paired-audio-v1" or data.get("origin") != "human_recording" or data.get("language") not in ("中文", "English") or not all(isinstance(data.get(field), str) and data[field].strip() for field in required):
                    raise YuanError("缺少人工录音与标注信息 / Human recording and annotation metadata are required")
                turns = data.get("turns")
                if not isinstance(turns, list) or not turns:
                    raise YuanError("数据集没有音频轮次 / Dataset has no audio turns")
                group = "dataset-" + payload_digest(data["recording_id"])
                split = data.get("split")
                if split is not None and split not in ("train", "validation", "guard", "release"):
                    raise YuanError("数据集划分无效 / Invalid dataset split")
                planned = []
                identifiers = set()
                for turn in turns:
                    check(cancel)
                    if not isinstance(turn, dict) or not isinstance(turn.get("id"), str) or not turn["id"] or turn["id"] in identifiers or turn.get("role") not in ("user", "assistant") or not isinstance(turn.get("speaker"), str) or not turn["speaker"].strip() or not is_digest(turn.get("sha256")):
                        raise YuanError("音频轮次标注无效 / Invalid audio turn annotation")
                    identifiers.add(turn["id"])
                    if turn.get("utterance_final") is not None and not isinstance(turn["utterance_final"], bool):
                        raise YuanError("语句结束标注必须为完整、截断或未知 / Utterance ending must be complete, truncated or unknown")
                    target = safe_path(self.root, turn["audio"])
                    if digest_file(target, cancel) != turn["sha256"]:
                        raise YuanError("数据集音频摘要不匹配 / Dataset audio digest mismatch")
                    info = sf.info(str(target))
                    if info.frames <= 0 or info.samplerate <= 0 or info.channels <= 0:
                        raise YuanError("数据集音频格式无效 / Invalid dataset audio format")
                    if info.frames * max(1, info.channels) * np.dtype(np.float32).itemsize > available_resources(psutil)["memory"] // max(2, self.model.layers + 1):
                        raise MemoryError("音频超过当前导入预算 / Audio exceeds the current import memory budget")
                    timestamp = turn.get("start_seconds")
                    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp) or timestamp < 0 or planned and timestamp < planned[-1][0]["start_seconds"]:
                        raise YuanError("音频轮次缺少有序时间标注 / Audio turns require ordered timestamps")
                    if turn["role"] == "assistant":
                        prior = next((item[0] for item in planned if item[0]["id"] == turn.get("reply_to")), None)
                        if prior is None or prior["role"] != "user" or prior["speaker"] == turn["speaker"] or turn.get("response_verified") is not True:
                            raise YuanError("回复关系或说话人未经标注 / Reply relation or speaker not annotated")
                    planned.append((turn, target))
                for turn, target in planned:
                    check(cancel)
                    waveform, rate = sf.read(str(target), dtype="float32", always_2d=True)
                    if digest_file(target, cancel) != turn["sha256"]:
                        raise InvalidAudio("导入期间音频发生变化 / Audio changed during import")
                    processed = self.model.normalize(waveform, int(rate))
                    prepared = self.store.prepare_audio(processed, self.model.rate, cancel)
                    staged.append((turn, prepared))
                    del waveform, processed
                if digest_file(path, cancel) != checksum:
                    raise YuanError("导入期间标注发生变化 / Annotations changed during import")
                with self.store.transaction():
                    self.store.group(group, "dataset", forced=split, source_key=data["recording_id"])
                    if self.store.setting(file_key) not in (None, key):
                        raise YuanError("同一数据集文件不能更换数据集身份 / A dataset file cannot change its dataset identity")
                    old_manifest = self.store.setting(key + "_manifest", {})
                    if old_manifest and old_manifest.get("recording_id") != data["recording_id"]:
                        raise YuanError("同一数据集文件不能更换录音来源身份 / A dataset file cannot change its recording identity")
                    changed = self.store.setting(key) not in (None, checksum)
                    if changed:
                        prior_pairs = self.store.db.execute("SELECT id,evidence FROM turn_pairs WHERE group_id=?", (group,)).fetchall()
                        for prior in prior_pairs:
                            if json.loads(prior["evidence"]).get("annotation_id") == old_manifest.get("annotation_id", data["annotation_id"]):
                                self.store.db.execute("UPDATE turn_pairs SET verified=0,eligible=0 WHERE id=?", (prior["id"],))
                        self.store.set_setting("data_revision", uuid.uuid4().hex)
                    recorded = {}
                    base_time = self.store.db.execute("SELECT created FROM groups WHERE id=?", (group,)).fetchone()[0]
                    for turn, prepared in staged:
                        check(cancel)
                        sample = self.store.add(group, turn["role"], None, self.model.rate, "dataset", cancel, created=base_time + turn["start_seconds"], sample_id=payload_digest([data["recording_id"], turn["id"]]),
                                                metadata={"recording_id": data["recording_id"], "speaker": turn["speaker"], "raw_sha256": turn["sha256"], "license": data["license"], "annotation": checksum, "annotation_id": data["annotation_id"], "utterance_final": turn.get("utterance_final")}, prepared=prepared)
                        recorded[turn["id"]] = (turn, self.store.sample(sample))
                        if turn["role"] == "assistant":
                            user_turn, user = recorded[turn["reply_to"]]
                            assistant = self.store.sample(sample)
                            evidence = {"method": "human_audio_annotation", "reviewer": data["reviewer"], "annotation_id": data["annotation_id"], "recording_id": data["recording_id"],
                                        "language": data["language"], "prompt": data["prompt"], "user_speaker": user_turn["speaker"], "assistant_speaker": turn["speaker"], "roles_verified": True,
                                        "response_verified": True, "user_sha256": user["digest"], "assistant_sha256": assistant["digest"], "annotation_sha256": checksum}
                            scenarios = turn.get("scenarios", data.get("scenarios", []))
                            if not isinstance(scenarios, list) or any(value not in AcceptancePolicy.dimensions for value in scenarios) or len(scenarios) != len(set(scenarios)):
                                raise YuanError("人工场景标注无效 / Invalid human scenario annotation")
                            evidence["scenarios"] = scenarios
                            controls = turn.get("contrasts", {})
                            if not isinstance(controls, dict) or any(value not in AcceptancePolicy.dimensions for value in controls):
                                raise YuanError("对照标注无效 / Invalid contrast annotation")
                            contrasts = {}
                            for dimension, control in controls.items():
                                if not isinstance(control, dict) or control.get("requires_different_reply") is not True or control.get("verified") is not True:
                                    raise YuanError("对照必须由人工确认需要不同回复 / Human confirmation that the contrast requires a different reply is required")
                                if dimension == "prompt":
                                    alternative = control.get("prompt")
                                    if not isinstance(alternative, str) or not alternative.strip() or alternative == data["prompt"]:
                                        raise YuanError("提示词对照无效 / Invalid prompt contrast")
                                    contrasts[dimension] = {"prompt": alternative}
                                elif dimension == "input":
                                    other = recorded.get(control.get("turn_id"))
                                    if other is None or other[0]["role"] != "user" or other[1]["id"] == user["id"]:
                                        raise YuanError("输入对照必须引用不同的真实用户音频 / Input contrast must reference different real user audio")
                                    contrasts[dimension] = {"sample": other[1]["id"]}
                                else:
                                    identifiers = control.get("turn_ids")
                                    if not isinstance(identifiers, list) or len(identifiers) != len(set(identifiers)) or any(identifier not in recorded for identifier in identifiers):
                                        raise YuanError("历史对照引用无效 / Invalid history contrast references")
                                    alternate = [recorded[identifier][1]["id"] for identifier in identifiers]
                                    if user["id"] in alternate or sample in alternate:
                                        raise YuanError("历史对照不能包含当前输入或目标 / History contrast cannot contain the current input or target")
                                    contrasts[dimension] = {"samples": alternate}
                            evidence["contrasts"] = contrasts
                            self.store.add_pair(group, user["id"], sample, "annotated_human_audio", verified=True, evidence=evidence)
                    retained = {sample[1]["id"] for sample in recorded.values()}
                    previous_rows = self.store.db.execute("SELECT id,metadata FROM samples WHERE group_id=?", (group,)).fetchall()
                    for previous in previous_rows:
                        if previous["id"] not in retained and (previous["id"] in old_manifest.get("samples", []) or json.loads(previous["metadata"]).get("annotation_id") == data["annotation_id"]):
                            self.store.db.execute("UPDATE samples SET learnable=0,dialogue_eligible=0,memory_eligible=0,exclusion='annotation_removed',memory_exclusion='annotation_removed' WHERE id=?", (previous["id"],))
                            self.store.db.execute("UPDATE turn_pairs SET verified=0,eligible=0 WHERE user_sample=? OR assistant_sample=?", (previous["id"], previous["id"]))
                            self.store.db.execute("DELETE FROM memories WHERE sample_id=?", (previous["id"],))
                    self.store.assert_independent([group])
                    self.store.set_setting(file_key, key)
                    self.store.set_setting(key, checksum)
                    self.store.set_setting(key + "_manifest", {"namespace": self.namespace, "dataset_id": data.get("dataset_id", path.name),
                                                              "recording_id": data["recording_id"], "annotation_id": data["annotation_id"], "samples": sorted(retained)})
                published = True
                self.failures.pop(path.name, None)
                self.emit("dataset_import", file=path.name, group=group, turns=len(recorded), annotation_sha256=checksum)
                return True
            except Cancelled:
                raise
            except (OSError, ValueError, KeyError, TypeError, RuntimeError, MemoryError) as exc:
                previous = self.failures.get(path.name, {})
                attempts = previous.get("attempts", 0) + 1 if previous.get("checksum") == checksum else 1
                delay = self.store.retry_delay(attempts)
                self.failures[path.name] = {"checksum": checksum, "attempts": attempts, "retry_at": time.monotonic() + delay}
                self.emit("notice", key="dataset_rejected", file=path.name, reason=concise_error(exc), retry_seconds=delay)
            finally:
                if not published:
                    for turn, prepared in staged:
                        self.store.discard_audio(prepared)
        return False


class PublicAudio:
    def __init__(self, state, store, model, emit):
        self.store, self.model, self.emit = store, model, emit
        self.root = safe_path(state, "sources")
        self.root.mkdir(exist_ok=True)
        tracked = {row[0] for row in self.store.db.execute("SELECT path FROM sources WHERE path IS NOT NULL")}
        pending = {row[0] for row in self.store.db.execute("SELECT id FROM sources WHERE finished=0")}
        for path in self.root.iterdir():
            if path.is_file() and path.name not in tracked and path.stem not in pending and len(path.stem) == hashlib.sha1().digest_size * 2 and all(char in "0123456789abcdef" for char in path.stem):
                path.unlink(missing_ok=True)
        self.next_network = 0.0
        self.failures = 0
        self.empty_queries = 0
        self.duration = model.chunk_seconds
        self.next_catalog = int(store.setting("source_catalog", 0))
        self.catalogs = (("Chinese", "acoustic", "Category:Spoken Chinese Wikipedia"),
                         ("English", "acoustic", "Category:Spoken English Wikipedia"),
                         ("Mixed", "dialogue", "Category:Audio files of interviews"))
        self.network = Network(policy=model.policy)

    def query(self, cancel):
        language, task, root = self.catalogs[self.next_catalog % len(self.catalogs)]
        key = "catalog_" + hashlib.sha256((language + "\0" + task + "\0" + root).encode()).hexdigest()[:16]
        catalog = self.store.setting(key, {})
        if not isinstance(catalog, dict) or not catalog.get("pending"):
            catalog = {"pending": [root], "visited": [], "continue": {}}
        current = catalog["pending"][0]
        continuation = catalog.get("continue", {})
        requested = self.model.policy.integer("network", "catalog_page_size", max(1, math.isqrt(self.model.capacity() // self.model.hop)), 1, max(1, self.model.capacity() // self.model.hop))
        params = {"action": "query", "format": "json", "generator": "categorymembers", "gcmtitle": current,
                  "gcmtype": "file|subcat", "gcmlimit": str(min(500, requested)),
                  "prop": "imageinfo|categories", "cllimit": "max", "iiprop": "url|size|mime|sha1|extmetadata"}
        if isinstance(continuation, dict):
            params.update({name: value for name, value in continuation.items() if name in ("continue", "gcmcontinue", "clcontinue", "iicontinue")})
        self.network.cancel = cancel
        result = self.network.json("https://commons.wikimedia.org/w/api.php?" + urllib.parse.urlencode(params))
        if not isinstance(result, dict) or "error" in result:
            raise YuanError("公开音频目录暂不可用 / Public audio catalog is temporarily unavailable")
        added = 0
        for page in result.get("query", {}).get("pages", {}).values():
            check(cancel)
            if page.get("ns") == 14:
                title = page.get("title")
                if isinstance(title, str) and title not in catalog["pending"] and title not in catalog["visited"]:
                    catalog["pending"].append(title)
                continue
            infos = page.get("imageinfo") or []
            if not infos:
                continue
            info = infos[0]
            metadata = info.get("extmetadata", {})
            categories = " ".join(str(item.get("title", "")) for item in page.get("categories", [])).lower()
            if any(value in categories for value in ("speech synthesis", "text-to-speech", "synthetic speech", "语音合成", "語音合成")):
                continue
            license_name = str(metadata.get("LicenseShortName", {}).get("value", "")).lower().replace("-", " ")
            license_parts = license_name.split()
            allowed_license = license_name in ("cc0", "cc0 1.0", "public domain", "public domain mark") or (
                license_parts[:2] == ["cc", "by"] and all(part == "sa" or part.replace(".", "", 1).isdigit() for part in license_parts[2:]))
            if not allowed_license:
                continue
            url = info.get("url", "")
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme != "https" or parsed.hostname != "upload.wikimedia.org" or not (str(info.get("mime", "")).startswith("audio/") or info.get("mime") == "application/ogg"):
                continue
            sha1 = str(info.get("sha1", "")).lower()
            if len(sha1) != 40 or any(char not in "0123456789abcdef" for char in sha1):
                try:
                    number = int(sha1, 36)
                    if number < 0 or number.bit_length() > 160:
                        continue
                    sha1 = format(number, "040x")
                except ValueError:
                    continue
            try:
                size = int(info.get("size", 0))
            except (ValueError, TypeError):
                continue
            if size <= 0:
                continue
            provenance = {"license": metadata.get("LicenseShortName", {}).get("value", ""), "license_url": metadata.get("LicenseUrl", {}).get("value", ""),
                          "artist": metadata.get("Artist", {}).get("value", ""), "attribution": metadata.get("Attribution", {}).get("value", ""),
                          "page": info.get("descriptionurl", ""), "title": page.get("title", ""), "size": size, "sha1": sha1, "mime": info.get("mime"),
                          "language": language, "task": task, "catalog": root}
            cursor = self.store.db.execute("INSERT OR IGNORE INTO sources(id,url,metadata,created) VALUES(?,?,?,?)", (sha1, url, json.dumps(provenance, ensure_ascii=False), time.time()))
            added += max(0, cursor.rowcount)
        self.store.db.commit()
        if result.get("continue"):
            catalog["continue"] = result["continue"]
        else:
            catalog["visited"].append(catalog["pending"].pop(0))
            catalog["continue"] = {}
        self.store.set_setting(key, catalog)
        self.next_catalog += 1
        self.store.set_setting("source_catalog", self.next_catalog)
        self.emit("source_catalog", language=language, task=task, category=current, added=added, pending=len(catalog["pending"]))
        return added

    def _dialogue_segments(self, waveform):
        np = self.model.np
        frame = self.model.hop
        frame_seconds = frame / self.model.rate
        min_seconds = min(self.model.chunk_seconds / 2, (self.model.layers + 2) * math.sqrt(frame_seconds * self.model.chunk_seconds))
        minimum = max(frame * 2, int(math.ceil(min_seconds * self.model.rate / frame)) * frame)
        max_seconds = max(self.model.chunk_seconds, self.model.capacity() / self.model.rate / 2)
        gate = SpeechGate(self.model.rate, frame, self.model.chunk_seconds, max_seconds)
        segments = []
        for start in range(0, len(waveform), frame):
            piece = waveform[start:start + frame]
            if len(piece) < frame:
                piece = np.pad(piece, (0, frame - len(piece)))
            result = gate.push(piece)
            if result is not None and len(result) >= minimum:
                segments.append(np.ascontiguousarray(result, dtype=np.float32))
        tail = gate.flush()
        if tail is not None and len(tail) >= minimum:
            segments.append(np.ascontiguousarray(tail, dtype=np.float32))
        return segments

    def _store_waveform(self, unfinished, waveform, offset, provenance, cancel):
        processed = self.model.normalize(waveform, int(provenance["rate"])) if provenance.get("rate") else self.model.normalize(waveform, self.model.rate)
        group_id = "public-" + unfinished["id"]
        base = {"source": unfinished["id"], "url": unfinished["url"], "page": provenance.get("page", ""), "title": provenance.get("title", ""),
                "license": provenance.get("license", ""), "license_url": provenance.get("license_url", ""), "artist": provenance.get("artist", ""),
                "attribution": provenance.get("attribution", ""), "recording_id": provenance.get("recording_id") or provenance.get("page") or unfinished["id"], "source_offset": unfinished["offset"], "source_end": offset, "source_task": provenance.get("task", "acoustic")}
        if provenance.get("task") != "dialogue":
            self.store.add(group_id, "reference", processed, self.model.rate, "public", cancel, metadata=base, task="acoustic")
            return 0
        segments = self._dialogue_segments(processed)
        ids = []
        lengths = []
        for index, segment in enumerate(segments):
            metadata = {**base, "utterance": index, "utterances": len(segments)}
            ids.append(self.store.add(group_id, "reference", segment, self.model.rate, "public", cancel, metadata=metadata, task="acoustic"))
            lengths.append(len(segment))
        pairs = 0
        for index in range(len(ids) - 1):
            left, right = lengths[index], lengths[index + 1]
            balance = math.sqrt(min(left, right) / max(left, right)) if max(left, right) else 0.0
            if balance <= 0:
                continue
            pair = self.store.add_pair(group_id, ids[index], ids[index + 1], "public_adjacent_audio", quality=balance, verified=False)
            pairs += 1
            self.emit("dialogue_pair", key="pair_candidate", pair=pair, group=group_id, source=unfinished["id"], quality=balance, verified=False)
        return pairs

    def _next_source(self, downloaded=False):
        return self.store.db.execute("SELECT * FROM sources WHERE finished=0 AND status IN ('pending','ready','retry') AND retry_at<=? AND " + ("path IS NOT NULL" if downloaded else "path IS NULL") + " ORDER BY retry_at,created,id LIMIT 1", (time.time(),)).fetchone()

    def _source_failure(self, row, exc, permanent=False):
        failures = int(row["failures"]) + 1
        delay = self.store.retry_delay(failures)
        retry_at = 0.0 if permanent else time.time() + delay
        status = "quarantined" if permanent else "retry"
        self.store.db.execute("UPDATE sources SET status=?,retry_at=?,failures=?,last_error=?,path=CASE WHEN ? THEN NULL ELSE path END WHERE id=?", (status, retry_at, failures, concise_error(exc), int(permanent), row["id"]))
        self.store.commit()
        if permanent:
            suffix = Path(urllib.parse.urlparse(row["url"]).path).suffix.lower()
            paths = [row["path"]] if row["path"] else []
            if suffix in (".flac", ".ogg", ".oga", ".wav", ".mp3", ".opus"):
                paths.extend((row["id"] + suffix, row["id"] + suffix + ".partial"))
            for name in set(paths):
                try:
                    safe_path(self.root, name).unlink(missing_ok=True)
                except (OSError, YuanError) as cleanup:
                    self.emit("notice", component="source_cleanup", source=row["id"], reason=concise_error(cleanup))
        self.emit("notice", key="source_skipped" if permanent else "source_retry", source=row["id"], status=status, failures=failures, retry_at=retry_at, retry_seconds=None if permanent else delay, reason=concise_error(exc))

    def step(self, cancel):
        import soundfile as sf
        import psutil
        check(cancel)
        storage = StorageBudget(self.model.state).snapshot(self.model.capacity() * self.model.np.dtype(self.model.np.float32).itemsize * 2)
        if not storage["ready"]:
            self.emit("network", key="learning_storage_pause", budget=storage)
            return False
        unfinished = self._next_source(downloaded=True)
        if unfinished is None and time.monotonic() >= self.next_network:
            began = time.monotonic()
            self.network.cancel = cancel
            missing = self._next_source()
            self.emit("network", key="network")
            if missing is None:
                try:
                    added = self.query(cancel)
                    self.empty_queries = self.empty_queries + 1 if not added else 0
                    self.failures = 0
                    missing = self._next_source()
                except Cancelled:
                    raise
                except Exception as exc:
                    self.failures += 1
                    self.emit("network", key="offline", component="catalog", reason=concise_error(exc))
            if missing is not None:
                try:
                    metadata = json.loads(missing["metadata"])
                    size = metadata["size"]
                    checksum = metadata["sha1"]
                    suffix = Path(urllib.parse.urlparse(missing["url"]).path).suffix.lower()
                    if isinstance(size, bool) or not isinstance(size, int) or size <= 0 or not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{40}", checksum) or suffix not in (".flac", ".ogg", ".oga", ".wav", ".mp3", ".opus"):
                        raise ValueError("公开音频元数据或格式无效 / Invalid public audio metadata or format")
                    available = min(available_resources(psutil)["memory"], StorageBudget(self.model.state).snapshot()["available_bytes"])
                    budget = self.model.policy.integer("network", "pending_audio_bytes", max(1, available // max(2, self.model.layers + 1)), 1, max(1, available))
                    if size > budget:
                        raise MemoryError("公开音频超过当前下载预算 / Public audio exceeds the current download budget")
                    target = safe_path(self.root, missing["id"] + suffix)
                    self.emit("source", url=missing["url"], metadata=metadata, file=target.name)
                    with StorageBudget(self.model.state).reserve(size):
                        self.network.download(missing["url"], target, size, checksum, algorithm="sha1", max_bytes=budget,
                                              progress=lambda done, total: self.emit("source_progress", key="network", current=done, total=total, unit="bytes", file=target.name))
                    check(cancel)
                    self.store.db.execute("UPDATE sources SET path=?,status='ready',retry_at=0,failures=0,last_error='' WHERE id=?", (target.name, missing["id"]))
                    self.store.commit()
                    unfinished = self.store.db.execute("SELECT * FROM sources WHERE id=?", (missing["id"],)).fetchone()
                    self.emit("network", key="available")
                except Cancelled:
                    raise
                except Exception as exc:
                    permanent = isinstance(exc, (ValueError, KeyError, TypeError, InvalidDownload)) or isinstance(exc, urllib.error.HTTPError) and exc.code in (400, 401, 403, 404, 410)
                    self._source_failure(missing, exc, permanent)
            measured = max(tick(), time.monotonic() - began)
            self.duration = math.sqrt(max(tick(), self.duration) * measured)
            if self._next_source() is not None:
                self.next_network = time.monotonic()
            else:
                ceiling = self.model.policy.number("network", "catalog_retry_max_seconds", max(self.model.chunk_seconds, self.network.timeout if hasattr(self.network, "timeout") else self.model.chunk_seconds), tick(), 86400.0)
                delay = min(ceiling, self.duration * (min(self.failures + self.empty_queries, math.sqrt(ceiling / self.duration)) + 1) ** 2)
                retry = self.store.db.execute("SELECT min(retry_at) FROM sources WHERE finished=0 AND status='retry'").fetchone()[0]
                if retry is not None:
                    delay = min(delay, max(tick(), retry - time.time()))
                self.next_network = time.monotonic() + delay
        if unfinished is None:
            return False
        path = safe_path(self.root, unfinished["path"])
        if not path.is_file():
            self.store.db.execute("UPDATE sources SET path=NULL,offset=0,status='pending',retry_at=0 WHERE id=?", (unfinished["id"],))
            self.store.commit()
            return False
        try:
            provenance = json.loads(unfinished["metadata"])
            with sf.SoundFile(str(path)) as stream:
                if unfinished["offset"] >= stream.frames:
                    self.store.db.execute("UPDATE sources SET finished=1,path=NULL,status='finished',retry_at=0 WHERE id=?", (unfinished["id"],))
                    self.store.commit()
                    path.unlink(missing_ok=True)
                    return False
                stream.seek(unfinished["offset"])
                factor = max(1.0, math.sqrt(self.model.layers + 1)) if provenance.get("task") == "dialogue" else 1.0
                take = max(1, int(self.model.chunk_seconds * factor * stream.samplerate))
                waveform = stream.read(take, dtype="float32", always_2d=True).mean(axis=1)
                offset = stream.tell()
                completed = offset >= stream.frames
                rate = int(stream.samplerate)
            check(cancel)
            if waveform.size:
                provenance["rate"] = rate
                self._store_waveform(unfinished, waveform, offset, provenance, cancel)
            self.store.db.execute("UPDATE sources SET offset=?,finished=?,path=?,status=?,retry_at=0,failures=0,last_error='' WHERE id=?", (offset, int(completed), None if completed else unfinished["path"], "finished" if completed else "ready", unfinished["id"]))
            self.store.commit()
            if completed:
                path.unlink(missing_ok=True)
            return True
        except Cancelled:
            raise
        except Exception as exc:
            permanent = isinstance(exc, (sf.LibsndfileError, ValueError, YuanError))
            self._source_failure(unfinished, exc, permanent)
            return False

class SampleRing:
    def __init__(self, capacity, rate=1):
        import numpy as np
        self.rate = float(rate)
        if not math.isfinite(self.rate) or self.rate <= 0:
            raise ValueError("采样率无效 / Invalid sample rate")
        self.np = np
        self.buffer = np.empty(max(1, int(capacity)), dtype=np.float32)
        self.read_index = self.write_index = self.count = 0
        self.lock = threading.Lock()
        self.overflows = self.dropped = self.discontinuities = 0
        self.timestamp = 0.0
        self.segments = deque()
        self.expected = None
        self.gap_pending = False

    def _discard(self, size):
        self.read_index = (self.read_index + size) % len(self.buffer)
        self.count -= size
        remaining = size
        while remaining:
            segment = self.segments[0]
            take = min(remaining, segment[0])
            segment[0] -= take
            segment[1] += take / self.rate
            remaining -= take
            if not segment[0]:
                self.segments.popleft()
        if self.segments:
            self.timestamp = self.segments[0][1]

    def write(self, values, timestamp=None, discontinuity=False):
        size = len(values)
        if not size:
            self.gap_pending = self.gap_pending or bool(discontinuity)
            return
        if not self.lock.acquire(blocking=False):
            self.overflows += 1
            self.dropped += size
            self.gap_pending = True
            return
        try:
            timestamp = time.monotonic() if timestamp is None else float(timestamp)
            if not math.isfinite(timestamp):
                self.overflows += 1
                self.dropped += size
                self.gap_pending = True
                return
            gap = self.gap_pending or bool(discontinuity)
            self.gap_pending = False
            if size > len(self.buffer):
                discarded = size - len(self.buffer)
                values = values[discarded:]
                timestamp += discarded / self.rate
                self.overflows += 1
                self.dropped += discarded
                size = len(values)
                gap = True
            tolerance = max(0.5 / self.rate, math.ulp(timestamp) * 2)
            gap = gap or self.expected is not None and abs(timestamp - self.expected) > tolerance
            missing = max(0, size - (len(self.buffer) - self.count))
            if missing:
                self.overflows += 1
                self.dropped += missing
                self._discard(missing)
                if self.segments:
                    self.segments[0][2] = True
                else:
                    gap = True
            if gap:
                self.discontinuities += 1
            first = min(size, len(self.buffer) - self.write_index)
            self.buffer[self.write_index:self.write_index + first] = values[:first]
            self.buffer[:size - first] = values[first:]
            self.write_index = (self.write_index + size) % len(self.buffer)
            self.count += size
            if self.segments and not gap:
                self.segments[-1][0] += size
            else:
                self.segments.append([size, timestamp, bool(gap)])
            self.expected = timestamp + size / self.rate
            self.timestamp = self.segments[0][1]
        finally:
            self.lock.release()

    def read_block(self, size):
        requested = min(len(self.buffer), max(0, int(size)))
        result = self.np.empty(requested, dtype=self.np.float32)
        with self.lock:
            take = min(requested, self.segments[0][0] if self.segments else 0)
            timestamp = self.segments[0][1] if self.segments else self.timestamp
            gap = bool(take and self.segments[0][2])
            first = min(take, len(self.buffer) - self.read_index)
            result[:first] = self.buffer[self.read_index:self.read_index + first]
            result[first:take] = self.buffer[:take - first]
            if take:
                self.segments[0][2] = False
                self._discard(take)
                if not self.segments:
                    self.timestamp = timestamp + take / self.rate
        return result[:take], timestamp, gap

    def read(self, size):
        values, timestamp, gap = self.read_block(size)
        return values, timestamp


class AudioPolicy:
    def __init__(self, model, devices, emit):
        import numpy as np
        import psutil
        self.policy = model.policy
        frame_seconds = model.hop / model.rate
        latency = max(frame_seconds, *(float(device.get("default_high_" + direction + "_latency", frame_seconds))
                                      for device, direction in zip(devices, ("input", "output"))))
        memory_seconds = available_resources(psutil)["memory"] / (model.rate * np.dtype(np.float32).itemsize)
        self.frame_size = model.hop
        self.segment_seconds = self.number("segment_seconds", min(model.capacity() // 2, model.input_budget or model.capacity() // 2) / model.rate, frame_seconds, min(model.capacity() / model.rate, memory_seconds / 16))
        self.pause_seconds = self.number("pause_seconds", (model.chunk_seconds ** 2 * frame_seconds) ** (1 / 3), frame_seconds, self.segment_seconds)
        self.onset_seconds = self.number("onset_seconds", math.sqrt(frame_seconds), frame_seconds, self.pause_seconds)
        self.buffer_seconds = self.number("buffer_seconds", max(model.chunk_seconds, latency * 2), frame_seconds, max(frame_seconds, memory_seconds / 16))
        self.echo_seconds = self.number("echo_seconds", max(latency * 2, self.pause_seconds), frame_seconds, self.segment_seconds)
        trials = max(1, self.echo_seconds * model.rate)
        self.echo_correlation = self.number("echo_correlation", min(1.0, math.sqrt(2 * math.log(trials / np.finfo(np.float32).eps) / self.frame_size)), np.finfo(np.float32).eps, 1.0)
        self.reconnect_seconds = self.number("reconnect_seconds", math.sqrt(model.chunk_seconds * latency), frame_seconds, model.capacity_seconds)
        self.latency = self.number("latency_seconds", math.sqrt(frame_seconds * latency), frame_seconds, max(frame_seconds, latency))
        self.policy.save()
        effective = dict(self.policy.effective.get("audio", {}))
        emit("audio_policy", **effective)

    def number(self, key, default, lower, upper):
        return self.policy.number("audio", key, default, lower, upper)

class SpeechGate:
    def __init__(self, rate, frame_size, chunk_seconds, max_seconds, pause_seconds=None, onset_seconds=None):
        self.rate, self.frame_size = int(rate), int(frame_size)
        self.frame_seconds = self.frame_size / self.rate
        self.pause = max(self.frame_seconds, pause_seconds or (chunk_seconds ** 2 * self.frame_seconds) ** (1 / 3))
        self.pause_limit = max(self.pause, chunk_seconds)
        self.max_frames = max(1, int(math.ceil(max_seconds / self.frame_seconds)))
        self.start_needed = max(1, int(math.ceil((onset_seconds or math.sqrt(self.frame_seconds)) / self.frame_seconds)))
        self.history = deque(maxlen=max(self.start_needed * 2, int(math.ceil(chunk_seconds / self.frame_seconds))))
        self.preroll = deque(maxlen=min(self.max_frames, max(self.start_needed, int(math.ceil(self.pause / self.frame_seconds)))))
        self.pauses = deque(maxlen=max(1, math.isqrt(self.max_frames)))
        self.bootstrap_spectrum = None
        self.frames = []
        self.silence = 0
        self.active = False
        self.limited = False
        self.start_count = 0
        self.confidence = 0.0

    def _threshold(self):
        import numpy as np
        if len(self.history) < self.start_needed:
            return math.inf
        values = np.asarray(self.history, dtype=np.float64)
        lower = values[values <= np.median(values)]
        median = float(np.median(lower))
        deviation = float(np.median(np.abs(lower - median)))
        floor = float(np.finfo(np.float32).eps * math.sqrt(self.rate))
        spread = max(deviation, median / max(1.0, math.sqrt(len(values))), floor)
        return median + spread * max(1.0, math.log2(len(values) + 1))

    def bootstrap_voice(self, frame, level):
        import numpy as np
        floor = float(np.finfo(np.float32).eps * math.sqrt(self.rate))
        if level <= floor:
            self.bootstrap_spectrum = None
            return False, 0.0
        if len(frame) < 4:
            return True, 1.0
        crossings = float(np.mean(np.signbit(frame[1:]) != np.signbit(frame[:-1])))
        window = np.hanning(len(frame)).astype(np.float64)
        power = np.abs(np.fft.rfft(frame.astype(np.float64) * window)) ** 2
        total = float(power.sum())
        if total <= np.finfo(np.float64).tiny:
            self.bootstrap_spectrum = None
            return False, 0.0
        distribution = power / total
        entropy = float(-(distribution * np.log(np.maximum(distribution, np.finfo(np.float64).tiny))).sum() / math.log(max(2, len(distribution))))
        spectrum = np.sqrt(distribution)
        if self.bootstrap_spectrum is None or len(self.bootstrap_spectrum) != len(spectrum):
            flux = 1.0
        else:
            flux = float(np.sqrt(np.mean((spectrum - self.bootstrap_spectrum) ** 2)))
        self.bootstrap_spectrum = spectrum
        zcr_limit = min(0.5, max(1 / len(frame), math.sqrt(self.frame_seconds)))
        entropy_limit = max(0.5, 1 - 1 / math.sqrt(max(2.0, math.log2(len(distribution) + 1))))
        flux_floor = 1 / max(2.0, math.sqrt(len(distribution)))
        voiced = crossings <= zcr_limit or entropy <= entropy_limit or flux >= flux_floor
        score = max(0.0, min(1.0, max((zcr_limit - crossings) / max(zcr_limit, np.finfo(np.float64).eps),
                                      (entropy_limit - entropy) / max(entropy_limit, np.finfo(np.float64).eps),
                                      flux / max(flux_floor, np.finfo(np.float64).eps) - 1)))
        return voiced, score

    def push(self, frame):
        import numpy as np
        frame = np.asarray(frame, dtype=np.float32).reshape(-1)
        if not frame.size or not np.isfinite(frame).all():
            raise YuanError("检测音频无效 / Invalid detector audio")
        frame = frame - frame.mean(dtype=np.float64)
        level = float(np.sqrt(np.mean(frame.astype(np.float64) ** 2)))
        threshold = self._threshold()
        floor = float(np.finfo(np.float32).eps * math.sqrt(self.rate))
        if math.isfinite(threshold):
            voiced = level > threshold
            self.confidence = max(0.0, min(1.0, (level - threshold) / max(floor, abs(threshold))))
        else:
            voiced, self.confidence = self.bootstrap_voice(frame, level)
        self.limited = False
        if not self.active:
            self.preroll.append(frame.copy())
            if not voiced:
                self.history.append(level)
            self.start_count = self.start_count + 1 if voiced else 0
            if self.start_count < self.start_needed:
                return None
            self.frames = list(self.preroll)
            self.preroll.clear()
            self.active = True
            self.silence = 0
            self.start_count = 0
        else:
            self.frames.append(frame.copy())
            if voiced:
                if self.silence:
                    self.pauses.append(self.silence * self.frame_seconds)
                    middle = sorted(self.pauses)[len(self.pauses) // 2]
                    self.pause = min(self.pause_limit, max(self.frame_seconds, math.sqrt(max(self.frame_seconds, middle) * self.pause) + self.frame_seconds))
                self.silence = 0
            else:
                self.silence += 1
                self.history.append(level)
        self.limited = len(self.frames) >= self.max_frames
        ended = self.silence * self.frame_seconds >= self.pause
        if not ended and not self.limited:
            return None
        silence = self.silence
        limited = self.limited
        if limited and not ended:
            result = np.concatenate(self.frames) if self.frames else None
            self.frames = []
            self.preroll.clear()
            self.silence = silence
            self.active = True
        else:
            result = self.flush()
        self.limited = limited and not ended
        return result

    def flush(self):
        import numpy as np
        frames = self.frames if self.active else list(self.preroll) if self.start_count else []
        trim = min(max(0, self.silence - 1), max(0, len(frames) - 1))
        if trim:
            frames = frames[:-trim]
        result = np.concatenate(frames) if frames else None
        self.frames = []
        self.preroll.clear()
        self.active = False
        self.silence = self.start_count = 0
        return result

class ReplyCancel:
    def __init__(self, parent, capture, epoch, allow_speaking=False):
        self.parent, self.capture, self.epoch = parent, capture, epoch
        self.allow_speaking = bool(allow_speaking)

    def is_set(self):
        return (self.parent.is_set() or self.capture.stop_event.is_set() or self.capture.reply_epoch != self.epoch
                or (self.capture.speaking and not self.allow_speaking) or not self.capture.connected)

    def wait(self, timeout):
        deadline = time.monotonic() + max(0.0, timeout)
        while not self.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            self.parent.wait(min(remaining, math.sqrt(tick())))
        return self.is_set()

class Playback:
    def __init__(self, waveform, reference, rate, reference_rate, cancel):
        self.waveform, self.reference = waveform, reference
        self.rate, self.reference_rate, self.cancel = rate, reference_rate, cancel
        self.cursor = 0
        self.started_at = None
        self.drain_at = None
        self.interrupted = False
        self.discontinuous = False

    def played(self):
        if self.started_at is None:
            return 0
        return max(0, min(self.cursor, int((time.monotonic() - self.started_at) * self.rate)))

class PlaybackResult:
    def __init__(self, waveform, rate, generated_samples, submitted_samples, played_samples, interrupted=False, discontinuous=False, underruns=0, error=None, started_at=None):
        self.waveform = waveform
        self.rate = int(rate)
        self.generated_samples = int(generated_samples)
        self.submitted_samples = int(submitted_samples)
        self.played_samples = int(played_samples)
        self.interrupted = bool(interrupted)
        self.discontinuous = bool(discontinuous)
        self.underruns = int(underruns)
        self.error = error
        self.started_at = started_at

    def metadata(self):
        return {"interrupted": self.interrupted, "discontinuous": self.discontinuous, "playback_underruns": self.underruns,
                "generated_samples": self.generated_samples, "submitted_samples": self.submitted_samples,
                "estimated_played_samples": self.played_samples, "sample_rate": self.rate,
                "submitted_to_device": self.submitted_samples > 0, "playback_error": self.error,
                "playback_extent": "estimated"}

class StreamingPlayback:
    def __init__(self, rate, reference_rate, cancel, buffer_samples, prebuffer_samples, reference_samples, timeline_samples=None):
        import numpy as np
        self.rate, self.reference_rate, self.cancel = int(rate), int(reference_rate), cancel
        self.capacity = max(1, int(buffer_samples))
        self.prebuffer = max(1, min(self.capacity, int(prebuffer_samples)))
        self._reference = np.empty(max(1, int(reference_samples)), dtype=np.float32)
        self.reference_count = 0
        self.items = deque()
        self.lock = threading.Lock()
        self.buffered = self.offset = self.cursor = 0
        self.started_at = self.drain_at = None
        self.interrupted = self.discontinuous = self.done = self.ready = False
        self.underruns = 0
        self.submitted = deque()
        self.audible_base = 0
        self.output_reference = OutputReference(self.rate, max(self.capacity, int(timeline_samples)) if timeline_samples is not None else max(self.capacity, math.ceil(reference_samples * self.rate / self.reference_rate)))

    @property
    def reference(self):
        return self._reference[:self.reference_count]

    def append_reference(self, values):
        size = len(values)
        if self.reference_count + size > len(self._reference):
            raise YuanError("播放参考超过内存边界 / Playback reference exceeds its memory boundary")
        self._reference[self.reference_count:self.reference_count + size] = values
        self.reference_count += size

    def push(self, values):
        with self.lock:
            if self.cancel.is_set() or self.interrupted or self.done or len(values) > self.capacity - self.buffered:
                return False
            if len(values):
                self.items.append(values)
                self.buffered += len(values)
            if self.buffered >= self.prebuffer:
                self.ready = True
        return True

    def finish(self):
        with self.lock:
            self.done = True
            self.ready = True

    def render(self, outdata, frames, timestamp):
        outdata.fill(0)
        if not self.lock.acquire(False):
            if self.started_at is not None:
                self.discontinuous = True
                self.underruns += 1
            self.output_reference.write(outdata[:, 0], timestamp)
            return
        try:
            if self.cancel.is_set() or self.interrupted:
                self.interrupted = True
                return
            now = time.monotonic()
            while self.submitted and self.submitted[0][1] + self.submitted[0][2] / self.rate <= now:
                start, _, size = self.submitted.popleft()
                self.audible_base = start + size
            if not self.ready:
                return
            filled = 0
            while filled < frames and self.items:
                values = self.items[0]
                size = min(frames - filled, len(values) - self.offset)
                outdata[filled:filled + size, :] = values[self.offset:self.offset + size, None]
                self.offset += size
                self.buffered -= size
                filled += size
                if self.offset == len(values):
                    self.items.popleft()
                    self.offset = 0
            if filled:
                if self.started_at is None:
                    self.started_at = timestamp
                self.submitted.append((self.cursor, timestamp, filled))
                self.cursor += filled
                self.drain_at = timestamp + filled / self.rate
            if filled < frames and not self.done:
                self.discontinuous = True
                self.underruns += 1
                self.ready = False
        finally:
            self.lock.release()
            self.output_reference.write(outdata[:, 0], timestamp)

    def played(self):
        with self.lock:
            total = self.audible_base
            now = time.monotonic()
            for start, timestamp, count in self.submitted:
                take = max(0, min(count, int((now - timestamp) * self.rate)))
                total = max(total, start + take)
                if take < count:
                    break
            return max(0, min(self.cursor, total))

    def result(self, values, rate, error=None):
        self.interrupted = self.interrupted or self.cancel.is_set()
        submitted = min(len(values), max(0, int(round(self.cursor * rate / self.rate))))
        complete = not self.interrupted and self.drained()
        played = len(values) if complete else min(submitted, max(0, int(self.played() * rate / self.rate)))
        if complete:
            submitted = len(values)
        return PlaybackResult(values[:played].copy(), rate, len(values), submitted, played, self.interrupted,
                              self.discontinuous, self.underruns, error, self.started_at)

    def drained(self):
        with self.lock:
            return self.done and self.buffered == 0 and (self.drain_at is None or time.monotonic() >= self.drain_at)

class OutputReference:
    def __init__(self, rate, capacity):
        import numpy as np
        self.rate = int(rate)
        if self.rate <= 0:
            raise ValueError("采样率无效 / Invalid sample rate")
        self.np = np
        self.values = np.zeros(max(1, int(capacity)), dtype=np.float32)
        self.stamps = np.full(len(self.values), -np.inf, dtype=np.float64)
        self.time_offsets = np.arange(len(self.values), dtype=np.float64) / self.rate
        self.count = 0
        self.lock = threading.Lock()
        self.latest = None
        self.dropped = 0

    def write(self, values, timestamp):
        np = self.np
        if not len(values) or not math.isfinite(timestamp):
            return
        if not self.lock.acquire(False):
            self.dropped += len(values)
            return
        try:
            take = min(len(values), len(self.values))
            source = values[-take:]
            start = timestamp + (len(values) - take) / self.rate
            offset = self.count % len(self.values)
            first = min(take, len(self.values) - offset)
            self.values[offset:offset + first] = source[:first]
            np.add(self.time_offsets[:first], start, out=self.stamps[offset:offset + first])
            if first < take:
                self.values[:take - first] = source[first:]
                np.add(self.time_offsets[first:take], start, out=self.stamps[:take - first])
            self.count += take
            self.latest = timestamp + len(values) / self.rate
        finally:
            self.lock.release()

    def window(self, start, count, rate):
        import numpy as np
        result = np.zeros(max(0, int(count)), dtype=np.float64)
        known = np.zeros(len(result), dtype=bool)
        if not len(result):
            return result, known
        end = start + len(result) / rate
        with self.lock:
            mask = (self.stamps >= start - 1 / self.rate) & (self.stamps <= end + 1 / self.rate)
            stamps, values = self.stamps[mask].copy(), self.values[mask].copy()
        if not len(stamps):
            return result, known
        order = np.argsort(stamps, kind="stable")
        stamps, values = stamps[order], values[order]
        times = start + np.arange(len(result)) / rate
        indices = np.searchsorted(stamps, times, side="right") - 1
        indices = np.clip(indices, 0, len(stamps) - 1)
        after = np.minimum(indices + 1, len(stamps) - 1)
        tolerance = max(0.51 / self.rate, math.ulp(max(abs(start), abs(end))) * 4)
        exact = np.abs(times - stamps[indices]) <= tolerance
        contiguous = (times >= stamps[indices]) & (times <= stamps[after]) & (stamps[after] - stamps[indices] <= 1.5 / self.rate)
        known = exact | contiguous
        spans = stamps[after] - stamps[indices]
        mix = np.divide(times - stamps[indices], spans, out=np.zeros(len(result)), where=spans > 0)
        result[known] = (values[indices] + np.clip(mix, 0, 1) * (values[after] - values[indices]))[known]
        return result, known

class EchoSuppressor:
    def __init__(self, rate, policy):
        self.rate, self.policy = rate, policy
        self.correlation = self.reduction = 0.0
        self.playback_identity = None
        self.gain = None
        self.delay = None
        self.double_talk = False

    def process(self, frame, timestamp, playback):
        import numpy as np
        self.correlation = self.reduction = 0.0
        self.double_talk = False
        if playback is None or playback.started_at is None or len(frame) < 2:
            return frame, False
        timeline = getattr(playback, "output_reference", None)
        last = timeline.latest if timeline is not None else playback.started_at + playback.cursor / playback.rate
        if last is None or timestamp + len(frame) / self.rate < playback.started_at or timestamp > last + self.policy.echo_seconds:
            return frame, False
        if self.playback_identity is not playback:
            self.playback_identity = playback
            self.gain = self.delay = None
        n = len(frame)
        lag = max(0, int(math.ceil(self.policy.echo_seconds * self.rate)))
        if timeline is not None:
            reference, known = timeline.window(timestamp - lag / self.rate, lag + n, self.rate)
        else:
            if playback.reference_rate != self.rate or playback.discontinuous:
                return frame, True
            position = int(round((timestamp - playback.started_at) * self.rate)) - lag
            reference = np.zeros(lag + n, dtype=np.float64)
            known = np.zeros(lag + n, dtype=bool)
            start, end = max(0, position), min(len(playback.reference), position + len(reference), int(playback.cursor * self.rate / playback.rate))
            if end > start:
                offset = start - position
                reference[offset:offset + end - start] = playback.reference[start:end]
                known[offset:offset + end - start] = True
        microphone = np.asarray(frame, dtype=np.float64)
        mean = float(microphone.mean())
        centered = microphone - mean
        energy = float(np.dot(centered, centered))
        epsilon = np.finfo(np.float32).eps ** 2 * n
        if energy <= epsilon:
            return frame, True
        size = 1 << (len(reference) + n - 2).bit_length()
        cross = np.fft.irfft(np.fft.rfft(reference, size) * np.fft.rfft(centered[::-1], size), size)[n - 1:len(reference)]
        cumulative = np.concatenate(([0.0], np.cumsum(reference)))
        squares = np.concatenate(([0.0], np.cumsum(reference * reference)))
        counts = np.concatenate(([0], np.cumsum(known)))
        sums = cumulative[n:] - cumulative[:-n]
        energies = np.maximum(0.0, squares[n:] - squares[:-n] - sums * sums / n)
        scores = np.abs(cross) / np.sqrt(np.maximum(np.finfo(np.float64).tiny, energies * energy))
        scores[(counts[n:] - counts[:-n] < n) | (energies <= epsilon)] = 0
        maximum = float(np.max(scores))
        tied = np.flatnonzero(np.isclose(scores, maximum, rtol=math.sqrt(np.finfo(np.float32).eps), atol=np.finfo(np.float32).eps))
        preferred = lag - (self.delay if self.delay is not None else 0)
        best = int(tied[int(np.argmin(np.abs(tied - preferred)))])
        self.correlation = min(1.0, float(scores[best]))
        if self.correlation < self.policy.echo_correlation:
            self.double_talk = True
            return frame, True
        selected = reference[best:best + n] - sums[best] / n
        estimate = float(cross[best] / energies[best])
        delay = lag - best
        residual_fraction = max(0.0, 1 - self.correlation ** 2)
        clean = residual_fraction <= min(1 - self.policy.echo_correlation, 1 / math.sqrt(n))
        if self.gain is None:
            if not clean:
                self.double_talk = True
                return frame, True
            self.gain, self.delay = estimate, delay
        elif clean:
            self.gain, self.delay = estimate, delay
        else:
            self.double_talk = True
            if abs(delay - self.delay) > max(1, round(n * (1 - self.correlation))):
                return frame, True
            if estimate * self.gain <= 0:
                return frame, True
            estimate = math.copysign(min(abs(estimate), abs(self.gain)), estimate)
        result = microphone - estimate * selected
        reduction = 1 - float(np.dot(result - mean, result - mean)) / energy
        if not math.isfinite(reduction) or reduction < 0:
            return frame, True
        self.reduction = min(1.0, reduction)
        return np.ascontiguousarray(result, dtype=np.float32), True

class DeviceExecutor:
    def __init__(self, emit):
        self.emit = emit
        self.requests = queue.Queue()
        self.lock = threading.Lock()
        self.failure = None
        self.closing = False
        self.thread = threading.Thread(target=self.run, daemon=True, name="YuanAudioDriver")
        self.thread.start()

    def poison(self, reason):
        with self.lock:
            if self.failure is None:
                self.failure = EngineFault(reason)
            self.closing = True
        return self.failure

    def call(self, key, function, timeout, cancel=None):
        with self.lock:
            if self.failure is not None:
                raise self.failure
            if self.closing:
                raise EngineFault("音频工作线程已关闭 / Audio driver worker is closed")
        if threading.current_thread() is self.thread:
            return function()
        check(cancel)
        deadline = time.monotonic() + max(tick(), timeout)
        done = threading.Event()
        result = []
        with operation(self.emit, key, scope="audio", budget_seconds=timeout):
            with self.lock:
                if self.failure is not None:
                    raise self.failure
                if self.closing:
                    raise EngineFault("音频工作线程已关闭 / Audio driver worker is closed")
                self.requests.put((function, done, result))
            while not done.wait(min(math.sqrt(tick()), max(0.0, deadline - time.monotonic()))):
                if time.monotonic() >= deadline:
                    raise self.poison("音频操作超时，需回收引擎 / Audio operation timed out; engine restart required: " + key)
            if time.monotonic() > deadline:
                raise self.poison("音频操作超时，需回收引擎 / Audio operation timed out; engine restart required: " + key)
            if isinstance(result[0], BaseException):
                raise result[0]
            check(cancel)
            return result[0]

    def run(self):
        while True:
            item = self.requests.get()
            if item is None:
                return
            function, done, result = item
            try:
                with self.lock:
                    failure = self.failure
                if failure is not None:
                    raise failure
                result.append(function())
            except BaseException as exc:
                result.append(exc)
            finally:
                done.set()

    def close(self, timeout):
        with self.lock:
            self.closing = True
            self.requests.put(None)
        self.thread.join(timeout=max(0.0, timeout))
        return not self.thread.is_alive()

class AudioDevices:
    def __init__(self, model, emit):
        import sounddevice as sd
        import soxr
        self.sd, self.soxr, self.model, self.emit = sd, soxr, model, emit
        self.streams = {}
        self.stream_lock = threading.RLock()
        self.faults = 0
        self.input_faults = 0
        self.input_callbacks = self.output_callbacks = 0
        self.last_input_callback = self.last_output_callback = time.monotonic()
        self.callback_timeout = model.policy.number("audio", "callback_timeout_seconds", max(1.0, model.chunk_seconds * 2), model.hop / model.rate, 3600.0)
        self.output_faults = 0
        self.vad_rate = model.rate
        self.frame_size = model.hop
        self.playback = None
        self.reference = None
        self.ring = None
        self.policy = None
        self.output_rate = model.rate
        self.input_rate = model.rate
        self.input_latency = self.output_latency = model.hop / model.rate
        self.capture = None
        self.driver = DeviceExecutor(emit)
        self.mono_buffer = None

    def timeout(self, key, default=None):
        default = max(5.0, self.model.chunk_seconds * 2) if default is None else default
        return self.model.policy.number("audio", key + "_seconds", default, max(tick(), 0.1), 3600.0)

    def cancel_playback(self):
        playback = self.playback
        if playback is not None:
            playback.interrupted = True
        self.playback = None

    def probe(self):
        return self.driver.call("audio_probe", self._probe, self.timeout("probe"))

    def open_streams(self, cancel):
        return self.driver.call("audio_open", lambda: self._open_streams(cancel), self.timeout("open"), cancel)

    def abort(self):
        self.cancel_playback()
        if self.driver.failure is None:
            return self.driver.call("audio_abort", self._abort, self.timeout("stop"))
        return False

    def close_streams(self):
        self.cancel_playback()
        if self.driver.failure is None:
            return self.driver.call("audio_close", self._close_streams, self.timeout("close"))
        return False

    def preflight(self, cancel):
        timeout = self.timeout("preflight")
        deadline = time.monotonic() + timeout
        return self.driver.call("audio_preflight", lambda: self._preflight(cancel, deadline), timeout, cancel)

    def defaults(self):
        devices = self.sd.query_devices()
        defaults = self.sd.default.device
        selected = []
        for position, direction in enumerate(("input", "output")):
            index = defaults[position]
            if index is None or index < 0 or index >= len(devices) or devices[index]["max_" + direction + "_channels"] < 1:
                options = [number for number, device in enumerate(devices) if device["max_" + direction + "_channels"] > 0]
                if not options:
                    raise YuanError("等待可用麦克风与扬声器 / Waiting for a microphone and speaker")
                index = options[0]
            selected.append(int(index))
        return selected, [devices[index] for index in selected]

    def _probe(self):
        defaults, _ = self.defaults()
        all_devices = self.sd.query_devices()
        indices, devices = [], []
        for default, direction in zip(defaults, ("input", "output")):
            options = [default, *[index for index, device in enumerate(all_devices) if index != default and device["max_" + direction + "_channels"] > 0]]
            selected = None
            reasons = []
            for index in options:
                device = dict(all_devices[index])
                maximum = int(device["max_" + direction + "_channels"])
                for channels in dict.fromkeys((1, min(2, maximum))):
                    for rate in dict.fromkeys((int(round(device["default_samplerate"])), self.model.rate)):
                        try:
                            getattr(self.sd, "check_" + direction + "_settings")(device=index, channels=channels, dtype="float32", samplerate=rate)
                            device.update(default_samplerate=rate, selected_channels=channels)
                            selected = index, device
                            break
                        except Exception as exc:
                            reasons.append(concise_error(exc))
                    if selected:
                        break
                if selected:
                    break
            if selected is None:
                raise YuanError("没有可用的音频设备配置 / No usable audio device configuration: " + "; ".join(dict.fromkeys(reasons)))
            indices.append(selected[0])
            devices.append(selected[1])
        self.emit("devices", input=devices[0]["name"], output=devices[1]["name"], input_rate=int(devices[0]["default_samplerate"]), output_rate=int(devices[1]["default_samplerate"]),
                  input_channels=devices[0]["selected_channels"], output_channels=devices[1]["selected_channels"], continuous=True)
        return indices, devices

    def _abort(self):
        playback = self.playback
        if playback is not None:
            playback.interrupted = True
        self.playback = None
        with self.stream_lock:
            streams = tuple(self.streams.values())
        for stream in streams:
            try:
                stream.abort(ignore_errors=True)
            except Exception:
                pass

    def _close_streams(self):
        with self.stream_lock:
            streams = tuple(self.streams.values())
            self.streams.clear()
        reasons = []
        for stream in streams:
            try:
                stream.abort(ignore_errors=True)
            except Exception as exc:
                reasons.append(concise_error(exc))
            try:
                stream.close(ignore_errors=True)
            except Exception as exc:
                reasons.append(concise_error(exc))
        if reasons:
            self.emit("notice", key="capture_recover", component="stream_cleanup", reason="; ".join(dict.fromkeys(reasons)))

    def timestamp(self, timing, key, fallback, frames=0, rate=None, discontinuity=False):
        now = time.monotonic()
        try:
            current = float(getattr(timing, "currentTime", 0.0))
            value = float(getattr(timing, key, 0.0))
        except (TypeError, ValueError, OverflowError):
            current = value = 0.0
        hardware = bool(current and value and math.isfinite(current) and math.isfinite(value))
        duration = frames / rate if frames > 0 and rate is not None and rate > 0 else 0.0
        if not hasattr(self, "clock_states"):
            self.clock_states = {}
        if not hasattr(self, "clock_offsets"):
            self.clock_offsets = {}
        previous = self.clock_states.get(key)
        if hardware:
            if previous is not None and not previous["hardware"] and not discontinuity:
                self.clock_offsets[key] = previous["next"] - value
            offset = self.clock_offsets.setdefault(key, now - current)
            timestamp = offset + value
        elif previous is not None and duration > 0 and not discontinuity:
            timestamp = previous["next"]
        else:
            timestamp = now + fallback
        self.clock_states[key] = {"next": timestamp + duration, "hardware": hardware, "arrival": now}
        return timestamp

    def input_callback(self, indata, frames, timing, status):
        self.last_input_callback = time.monotonic()
        self.input_callbacks += 1
        input_fault = bool(status and (status.input_overflow or status.input_underflow))
        if input_fault:
            self.input_faults += 1
            self.faults += 1
        timestamp = self.timestamp(timing, "inputBufferAdcTime", -max(self.input_latency, frames / self.input_rate), frames, self.input_rate, input_fault)
        if indata.shape[1] == 1:
            self.ring.write(indata[:, 0], timestamp, discontinuity=input_fault)
        else:
            for offset in range(0, frames, len(self.mono_buffer)):
                count = min(len(self.mono_buffer), frames - offset)
                values = self.mono_buffer[:count]
                self.model.np.mean(indata[offset:offset + count], axis=1, out=values)
                self.ring.write(values, timestamp + offset / self.input_rate, discontinuity=input_fault and offset == 0)

    def output_callback(self, outdata, frames, timing, status):
        self.last_output_callback = time.monotonic()
        self.output_callbacks += 1
        outdata.fill(0)
        output_fault = bool(status and (status.output_underflow or status.output_overflow))
        if output_fault:
            self.output_faults += 1
            self.faults += 1
        timestamp = self.timestamp(timing, "outputBufferDacTime", self.output_latency, frames, self.output_rate, output_fault)
        playback = self.playback
        if playback is None:
            return
        if output_fault:
            playback.discontinuous = True
        if playback.cancel.is_set() or playback.interrupted:
            playback.interrupted = True
            if isinstance(playback, StreamingPlayback):
                playback.output_reference.write(outdata[:, 0], timestamp)
            return
        if status and status.output_overflow:
            return
        if isinstance(playback, StreamingPlayback):
            playback.render(outdata, frames, timestamp)
            return
        count = min(frames, len(playback.waveform) - playback.cursor)
        if count <= 0:
            return
        start = playback.cursor
        if playback.started_at is None:
            playback.started_at = timestamp
        outdata[:count, :] = playback.waveform[start:start + count, None]
        playback.cursor += count
        playback.drain_at = timestamp + count / playback.rate

    def duplex_callback(self, indata, outdata, frames, timing, status):
        self.output_callback(outdata, frames, timing, status)
        self.input_callback(indata, frames, timing, status)

    def _open_streams(self, cancel):
        indices, devices = self._probe()
        self.clock_offsets = {}
        self.clock_states = {}
        self.policy = AudioPolicy(self.model, devices, self.emit)
        self.playback = self.reference = None
        self.last_input_callback = self.last_output_callback = time.monotonic()
        reasons = []
        common_rates = dict.fromkeys((int(devices[0]["default_samplerate"]), int(devices[1]["default_samplerate"]), self.model.rate))
        for rate in common_rates:
            check(cancel)
            stream = None
            try:
                self.input_rate = self.output_rate = rate
                self.clock_offsets = {}
                self.clock_states = {}
                self.ring = SampleRing(math.ceil(rate * self.policy.buffer_seconds), rate)
                self.mono_buffer = self.model.np.empty(len(self.ring.buffer), dtype=self.model.np.float32)
                stream = self.sd.Stream(device=tuple(indices), samplerate=rate, channels=tuple(device["selected_channels"] for device in devices), dtype="float32",
                                        blocksize=0, latency=self.policy.latency, callback=self.duplex_callback, never_drop_input=True)
                reported = stream.latency
                self.input_latency, self.output_latency = reported if isinstance(reported, (tuple, list)) else (reported, reported)
                with self.stream_lock:
                    check(cancel)
                    self.streams["duplex"] = stream
                stream.start()
                self.emit("devices", input_rate=rate, output_rate=rate, input_latency=self.input_latency, output_latency=self.output_latency, transport="duplex", continuous=True)
                return
            except Cancelled:
                if stream is not None:
                    stream.close(ignore_errors=True)
                self.close_streams()
                raise
            except Exception as exc:
                reasons.append(concise_error(exc))
                if stream is not None:
                    stream.close(ignore_errors=True)
                with self.stream_lock:
                    self.streams.clear()
        self.input_rate = int(devices[0]["default_samplerate"])
        self.output_rate = int(devices[1]["default_samplerate"])
        self.ring = SampleRing(math.ceil(self.input_rate * self.policy.buffer_seconds), self.input_rate)
        self.mono_buffer = self.model.np.empty(len(self.ring.buffer), dtype=self.model.np.float32)
        try:
            for direction, index, device in zip(("input", "output"), indices, devices):
                check(cancel)
                constructor = self.sd.InputStream if direction == "input" else self.sd.OutputStream
                stream = constructor(device=index, samplerate=int(device["default_samplerate"]), channels=device["selected_channels"], dtype="float32",
                                     blocksize=0, latency=self.policy.latency, callback=self.input_callback if direction == "input" else self.output_callback)
                setattr(self, direction + "_latency", float(stream.latency))
                with self.stream_lock:
                    self.streams[direction] = stream
                    check(cancel)
                stream.start()
            self.emit("devices", input_rate=self.input_rate, output_rate=self.output_rate, input_latency=self.input_latency, output_latency=self.output_latency, transport="separate-continuous", continuous=True)
        except BaseException:
            self.close_streams()
            raise
        self.emit("notice", key="full_duplex", component="separate_streams", reasons=reasons)

    def _preflight(self, cancel, deadline):
        self._close_streams()
        initial_input, initial_output = self.input_callbacks, self.output_callbacks
        try:
            self._open_streams(cancel)
            while time.monotonic() < deadline:
                check(cancel)
                if not self.active():
                    raise YuanError("音频流未保持运行 / Audio streams did not remain active")
                if self.input_callbacks > initial_input and self.output_callbacks > initial_output:
                    self.emit("devices", probed=True, continuous=False)
                    return True
                cancel.wait(min(math.sqrt(tick()), max(0.0, deadline - time.monotonic())))
            raise YuanError("音频设备未在期限内提供输入输出回调 / Audio devices did not provide input and output callbacks in time")
        finally:
            self._close_streams()
            self.ring = None

    def active(self):
        if self.driver.failure is not None:
            return False
        with self.stream_lock:
            opened = bool(self.streams)
        now = time.monotonic()
        return opened and now - self.last_input_callback <= self.callback_timeout and now - self.last_output_callback <= self.callback_timeout

    def play(self, waveform, rate, cancel):
        import numpy as np
        check(cancel)
        original = np.asarray(waveform, dtype=np.float32).reshape(-1)
        if not original.size or not np.isfinite(original).all():
            raise YuanError("播放音频无效 / Invalid playback audio")
        if not self.active():
            raise YuanError("持续音频流不可用 / Continuous audio stream is unavailable")
        values = self.soxr.resample(original, rate, self.output_rate).astype(np.float32) if rate != self.output_rate else original.copy()
        values = np.ascontiguousarray(np.clip(values, -1, 1), dtype=np.float32)
        reference = self.soxr.resample(values, self.output_rate, self.vad_rate).astype(np.float32) if self.output_rate != self.vad_rate else values
        playback = Playback(values, reference, self.output_rate, self.vad_rate, cancel)
        self.reference = self.playback = playback
        self.emit("status", key="speaking", id=self.capture.session_id if self.capture else None)
        interval = max(self.frame_size / self.vad_rate, math.sqrt(tick()))
        try:
            while True:
                if cancel.is_set():
                    playback.interrupted = True
                    break
                if not self.active():
                    playback.interrupted = True
                    raise YuanError("持续音频流已中断 / Continuous audio stream was interrupted")
                if playback.cursor == len(values) and playback.drain_at is not None and time.monotonic() >= playback.drain_at:
                    break
                cancel.wait(interval)
        finally:
            if self.playback is playback:
                self.playback = None
        played = min(len(original), int(playback.played() * rate / self.output_rate)) if playback.interrupted else len(original)
        self.emit("audio", id=self.capture.session_id if self.capture else None, output_seconds=played / rate,
                  interrupted=playback.interrupted, playback_discontinuity=playback.discontinuous, continuous=self.active())
        return original[:played].copy(), playback.interrupted


    def play_stream(self, chunks, rate, cancel, max_samples=None):
        import numpy as np
        check(cancel)
        if not self.active():
            raise YuanError("持续音频流不可用 / Continuous audio stream is unavailable")
        capacity = self.model.capacity() if max_samples is None else max(1, int(max_samples))
        duration = capacity / rate
        frame_seconds = self.frame_size / self.vad_rate
        section = getattr(self.model, "performance_section", "performance")
        realtime = self.model.policy.measured(section, "generation_realtime_factor", 1.0)
        underruns = self.model.policy.measured(section, "playback_underruns", 0.0)
        baseline = max(frame_seconds, self.output_latency * (1 + math.sqrt(1 + underruns)))
        seconds = self.model.policy.number("audio", "playback_buffer_seconds", max(baseline, self.model.chunk_seconds), frame_seconds, max(frame_seconds, duration))
        prebuffer = self.model.policy.number("audio", "playback_prebuffer_seconds", min(seconds, max(baseline, seconds * max(0.0, 1 - 1 / max(1.0, realtime)))), frame_seconds, seconds)
        playback = StreamingPlayback(self.output_rate, self.vad_rate, cancel, math.ceil(seconds * self.output_rate), math.ceil(prebuffer * self.output_rate),
                                     math.ceil(duration * self.vad_rate) + self.frame_size,
                                     timeline_samples=math.ceil((self.policy.echo_seconds + self.policy.buffer_seconds + seconds) * self.output_rate))
        resampler = self.soxr.ResampleStream(rate, self.output_rate, 1, dtype="float32") if rate != self.output_rate else None
        reference_resampler = self.soxr.ResampleStream(self.output_rate, self.vad_rate, 1, dtype="float32") if self.output_rate != self.vad_rate else None
        self.reference = self.playback = playback
        self.last_playback_error = None
        self.last_playback_started = None
        original = []
        generated = 0
        iterator = iter(chunks)
        interval = max(frame_seconds, math.sqrt(tick()))
        announced = False
        def observe_start():
            nonlocal announced
            if not announced and playback.started_at is not None:
                announced = True
                self.last_playback_started = playback.started_at
                self.emit("status", key="speaking", id=self.capture.session_id if self.capture else None, streaming=True)
        def queue_values(values, last=False):
            check(cancel)
            reference = reference_resampler.resample_chunk(values, last=last) if reference_resampler is not None else values
            playback.append_reference(reference)
            offset = 0
            while offset < len(values):
                observe_start()
                check(cancel)
                if playback.interrupted:
                    raise Cancelled()
                if not self.active():
                    raise YuanError("持续音频流已中断 / Continuous audio stream was interrupted")
                with playback.lock:
                    available = playback.capacity - playback.buffered
                if available:
                    piece = np.ascontiguousarray(values[offset:offset + available], dtype=np.float32)
                    piece.setflags(write=False)
                    if playback.push(piece):
                        offset += len(piece)
                        continue
                cancel.wait(interval)
            observe_start()
        with operation(self.emit, "stream_playback", "conversation", budget_seconds=duration * (1 + max(1.0, realtime)) + seconds):
            try:
                for chunk in iterator:
                    check(cancel)
                    values = np.asarray(chunk, dtype=np.float32).reshape(-1)
                    if not values.size:
                        continue
                    if not np.isfinite(values).all() or generated + len(values) > capacity:
                        raise YuanError("播放片段无效或超过回复边界 / Invalid playback chunk or reply boundary exceeded")
                    values = np.ascontiguousarray(np.clip(values, -1, 1), dtype=np.float32)
                    original.append(values)
                    generated += len(values)
                    converted = resampler.resample_chunk(values) if resampler is not None else values
                    queue_values(converted)
                tail = resampler.resample_chunk(np.empty(0, dtype=np.float32), last=True) if resampler is not None else np.empty(0, dtype=np.float32)
                queue_values(tail, last=True)
                if not generated:
                    raise YuanError("模型没有提供播放音频 / Model provided no playback audio")
                playback.finish()
                while not playback.drained():
                    observe_start()
                    check(cancel)
                    if playback.interrupted:
                        raise Cancelled()
                    if not self.active():
                        raise YuanError("持续音频流已中断 / Continuous audio stream was interrupted")
                    cancel.wait(interval)
                observe_start()
            except Cancelled:
                playback.interrupted = True
            except Exception as exc:
                playback.interrupted = True
                self.last_playback_error = concise_error(exc)
                self.emit("notice", key="playback_gap", component="streamed_reply", reason=self.last_playback_error)
            finally:
                if self.playback is playback:
                    self.playback = None
                close = getattr(iterator, "close", None)
                if close is not None:
                    try:
                        close()
                    except Exception as exc:
                        playback.interrupted = True
                        self.last_playback_error = concise_error(exc)
                        self.emit("notice", key="playback_gap", component="generation_cleanup", reason=self.last_playback_error)
                self.model.policy.observe(section, "playback_underruns", playback.underruns)
            values = np.concatenate(original) if original else np.empty(0, dtype=np.float32)
            result = playback.result(values, rate, self.last_playback_error)
            self.emit("audio", id=self.capture.session_id if self.capture else None, output_seconds=result.played_samples / rate, streaming=True,
                      **result.metadata(), playback_discontinuity=result.discontinuous, underruns=result.underruns,
                      buffer_seconds=seconds, prebuffer_seconds=prebuffer, continuous=self.active())
            return result

class AudioPacketQueue:
    def __init__(self, byte_limit, item_limit):
        self.byte_limit = max(1, int(byte_limit))
        self.item_limit = max(1, int(item_limit))
        self.items = deque()
        self.bytes = 0
        self.closed = False
        self.rejected_utterance = None
        self.condition = threading.Condition()

    def put(self, packet, replace_oldest=False, preserve_utterance=False):
        size = int(packet["waveform"].nbytes)
        dropped = []
        def identity(item):
            return item.get("group_id"), item.get("metadata", {}).get("utterance_id")
        with self.condition:
            if self.closed:
                return False, dropped
            identifier = identity(packet) if preserve_utterance else None
            if preserve_utterance and not identifier[1]:
                raise ValueError("推理音频缺少语句身份 / Inference audio lacks an utterance identity")
            if preserve_utterance and self.rejected_utterance == identifier:
                return False, dropped
            if preserve_utterance:
                self.rejected_utterance = None
            def discard_utterance(target):
                retained = deque()
                for item in self.items:
                    if identity(item) == target:
                        self.bytes -= int(item["waveform"].nbytes)
                        dropped.append(item)
                    else:
                        retained.append(item)
                self.items = retained
                self.condition.notify_all()
            if size > self.byte_limit:
                if preserve_utterance:
                    discard_utterance(identifier)
                    self.rejected_utterance = identifier
                return False, dropped
            while self.items and (self.bytes + size > self.byte_limit or len(self.items) >= self.item_limit):
                if not replace_oldest:
                    return False, dropped
                if preserve_utterance:
                    oldest = identity(self.items[0])
                    discard_utterance(oldest)
                    if oldest == identifier:
                        self.rejected_utterance = identifier
                        return False, dropped
                else:
                    removed = self.items.popleft()
                    self.bytes -= int(removed["waveform"].nbytes)
                    dropped.append(removed)
            self.items.append(packet)
            self.bytes += size
            self.condition.notify_all()
            return True, dropped

    def get(self, timeout):
        with self.condition:
            if not self.items and not self.closed:
                self.condition.wait(timeout)
            if not self.items:
                return None
            packet = self.items.popleft()
            self.bytes -= int(packet["waveform"].nbytes)
            self.condition.notify_all()
            return packet

    def pending(self):
        with self.condition:
            return len(self.items)

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()

class AudioWriter:
    def __init__(self, model, emit):
        import psutil
        self.model, self.emit = model, emit
        minimum = max(1, model.capacity() * model.np.dtype(model.np.float32).itemsize)
        byte_limit = model.policy.integer("audio", "storage_queue_bytes", minimum * 2, minimum, max(minimum, available_resources(psutil)["memory"] // 8))
        item_limit = model.policy.integer("audio", "storage_queue_segments", max(2, math.isqrt(max(1, model.capacity() // model.hop))), 1, max(2, model.capacity() // model.hop))
        self.retry_limit = model.policy.integer("storage", "write_attempts", 3, 1, 64)
        self.retry_base = model.policy.number("storage", "write_retry_seconds", max(0.1, model.chunk_seconds), tick(), 3600.0)
        self.retry_max = model.policy.number("storage", "write_retry_max_seconds", max(self.retry_base, model.chunk_seconds * 4), self.retry_base, 86400.0)
        self.session_limit = model.policy.integer("storage", "session_stat_history", max(8, item_limit * 2), 1, 65536)
        StorageBudget(model.state).configure(model, byte_limit)
        self.queue = AudioPacketQueue(byte_limit, item_limit)
        self.closing = threading.Event()
        self.finished = threading.Event()
        self.condition = threading.Condition(threading.RLock())
        self.inflight = self.retrying = False
        self.accepted = self.completed = self.saved = self.failed = self.rejected = self.retries = 0
        self.lost_seconds = 0.0
        self.failure = self.fatal = None
        self.storage_available = False
        self.sessions = {}
        self.active_packet = None
        self.restarts = 0
        self.thread = threading.Thread(target=self.run, daemon=True, name="YuanAudioWriter")
        self.thread.start()

    def _session(self, session_id):
        value = self.sessions.get(session_id)
        if value is None:
            value = {"accepted": 0, "completed": 0, "saved": 0, "failed": 0, "rejected": 0, "lost_seconds": 0.0, "error": None, "sealed": False}
            self.sessions[session_id] = value
        return value

    def enqueue(self, packet):
        with self.condition:
            stats = self._session(packet["group_id"])
            if self.closing.is_set() or self.fatal is not None or stats["sealed"]:
                accepted = False
            else:
                accepted, _ = self.queue.put(dict(packet))
            if accepted:
                self.accepted += 1
                stats["accepted"] += 1
            else:
                duration = len(packet["waveform"]) / packet["rate"]
                self.rejected += 1
                self.lost_seconds += duration
                stats["rejected"] += 1
                stats["lost_seconds"] += duration
                stats["error"] = self.fatal or "存档队列未接收片段 / Storage queue did not accept the segment"
            self.condition.notify_all()
        if not accepted:
            self.emit("notice", key="storage_overload", id=packet["group_id"], sample=packet["id"], lost_seconds=len(packet["waveform"]) / packet["rate"], pending=self.queue.pending())
        return accepted

    def _complete(self, packet, saved, error=None):
        with self.condition:
            stats = self._session(packet["group_id"])
            self.completed += 1
            stats["completed"] += 1
            if saved:
                self.saved += 1
                stats["saved"] += 1
                self.storage_available = True
                self.failure = None
            else:
                duration = len(packet["waveform"]) / packet["rate"]
                self.failed += 1
                self.lost_seconds += duration
                stats["failed"] += 1
                stats["lost_seconds"] += duration
                stats["error"] = error or self.failure
            self.active_packet = None
            self.inflight = self.retrying = False
            self.condition.notify_all()
        self.emit("storage_state", id=packet["group_id"], session=self.session_status(packet["group_id"]), **self.status())

    def _close_store(self, store):
        if store is not None:
            try:
                store.close()
            except (OSError, sqlite3.Error) as exc:
                self.emit("notice", key="storage_retry", component="storage_close", reason=concise_error(exc))

    def _unavailable(self, exc, attempt, retryable=True):
        delay = min(self.retry_max, self.retry_base * min(attempt, math.sqrt(self.retry_max / self.retry_base)) ** 2)
        with self.condition:
            self.failure = concise_error(exc)
            self.storage_available = False
            self.retrying = retryable
            self.retries += int(retryable)
            if not retryable:
                self.fatal = self.failure
            self.condition.notify_all()
        self.emit("notice", key="storage_retry" if retryable else "storage_failed", component="storage_connection", reason=self.failure,
                  attempt=attempt, retry_seconds=delay if retryable else None, recoverable=retryable)
        self.emit("storage_state", **self.status())
        return delay

    def _probe(self, store):
        fd, name = tempfile.mkstemp(dir=store.audio, prefix=".probe-")
        try:
            with os.fdopen(fd, "wb") as probe:
                probe.write(b"Yuan")
                probe.flush()
                os.fsync(probe.fileno())
            store.set_setting("writer_probe", time.time())
        finally:
            Path(name).unlink(missing_ok=True)

    def run(self):
        store = None
        connections = 0
        try:
            while not self.closing.is_set() or self.queue.pending() or self.active_packet is not None:
                if store is None:
                    if self.closing.is_set():
                        break
                    try:
                        store = AudioStore(self.model.state, recover=False)
                        self._probe(store)
                        connections = 0
                        with self.condition:
                            self.storage_available = True
                            self.retrying = False
                            self.failure = None
                            self.condition.notify_all()
                        self.emit("storage_state", **self.status())
                    except Exception as exc:
                        self._close_store(store)
                        store = None
                        connections += 1
                        retryable = isinstance(exc, (OSError, sqlite3.OperationalError)) and not isinstance(exc, InvalidAudio)
                        delay = self._unavailable(exc, connections, retryable)
                        if not retryable:
                            break
                        self.closing.wait(delay)
                        continue
                packet = self.active_packet or self.queue.get(math.sqrt(tick()))
                if packet is None:
                    continue
                with self.condition:
                    self.active_packet = packet
                    self.inflight = True
                saved = False
                error = None
                for attempt in range(self.retry_limit):
                    try:
                        if store is None:
                            store = AudioStore(self.model.state, recover=False)
                        sample = store.add(packet["group_id"], packet["role"], packet["waveform"], packet["rate"], "local", generated=packet["role"] == "assistant",
                                           eligible=packet.get("eligible", True), created=packet["created"], metadata=packet["metadata"], sample_id=packet["id"])
                        saved = True
                        if packet["role"] == "assistant":
                            input_id = packet["metadata"].get("input_sample")
                            try:
                                if input_id and store.sample(input_id) is not None:
                                    pair = store.add_pair(packet["group_id"], input_id, sample, "yuan_generated", verified=False)
                                    self.emit("dialogue_pair", pair=pair, group=packet["group_id"], source="yuan_generated", verified=False)
                            except Exception as exc:
                                self.emit("notice", component="history_pair", sample=sample, input_sample=input_id, audio_saved=True, reason=concise_error(exc))
                        self.emit("capture_saved", id=packet["group_id"], sample=sample, role=packet["role"], seconds=len(packet["waveform"]) / packet["rate"],
                                  learnable=packet["role"] != "assistant" and packet.get("eligible", True), pending_storage=self.queue.pending())
                        break
                    except Exception as exc:
                        if saved:
                            self.emit("notice", component="storage_event", sample=packet["id"], audio_saved=True, reason=concise_error(exc))
                            break
                        error = concise_error(exc)
                        retryable = isinstance(exc, (OSError, sqlite3.OperationalError)) and not isinstance(exc, InvalidAudio)
                        self._close_store(store)
                        store = None
                        retry = retryable and attempt + 1 < self.retry_limit and not self.closing.is_set()
                        delay = self._unavailable(exc, attempt + 1, retryable)
                        self.emit("notice", key="storage_retry" if retry else "storage_failed", id=packet["group_id"], sample=packet["id"], reason=error,
                                  attempt=attempt + 1, retry_seconds=delay if retry else None, lost_seconds=0.0 if retry else len(packet["waveform"]) / packet["rate"])
                        if not retry:
                            break
                        self.closing.wait(delay)
                self._complete(packet, saved, error)
                if self.fatal is not None:
                    break
        except Exception as exc:
            with self.condition:
                self.failure = concise_error(exc)
                self.storage_available = False
            self.emit("notice", key="storage_failed", component="writer_worker", reason=self.failure, recoverable=True)
        finally:
            self._close_store(store)
            if self.closing.is_set() or self.fatal is not None:
                if self.active_packet is not None:
                    self._complete(self.active_packet, False, self.failure or "存档任务已关闭 / Storage worker closed")
                while True:
                    packet = self.queue.get(0)
                    if packet is None:
                        break
                    self._complete(packet, False, self.failure or "存档任务已关闭 / Storage worker closed")
            self.finished.set()
            with self.condition:
                self.storage_available = False
                self.condition.notify_all()

    def ensure_running(self):
        with self.condition:
            if self.closing.is_set() or self.fatal is not None or self.thread.is_alive():
                return
            self.finished.clear()
            self.restarts += 1
            self.thread = threading.Thread(target=self.run, daemon=True, name="YuanAudioWriter")
            self.thread.start()
        self.emit("notice", key="storage_retry", component="writer_restart", restarts=self.restarts)

    def session_status(self, session_id):
        with self.condition:
            row = dict(self._session(session_id))
            return {**row, "pending": max(0, row["accepted"] - row["completed"])}

    def flush_session(self, session_id, timeout):
        deadline = time.monotonic() + max(0.0, timeout)
        self.ensure_running()
        with self.condition:
            stats = self._session(session_id)
            stats["sealed"] = True
            target = stats["accepted"]
            while stats["completed"] < target and not self.finished.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.condition.wait(min(remaining, math.sqrt(tick())))
            pending = max(0, target - stats["completed"])
            result = FlushResult(ok=pending == 0 and stats["saved"] >= target and not stats["failed"] and not stats["rejected"], session_id=session_id,
                                 target=target, saved=stats["saved"], failed=stats["failed"], rejected=stats["rejected"], pending=pending,
                                 retrying=self.retrying and pending > 0, lost_seconds=stats["lost_seconds"], error=stats["error"] or (self.failure if pending else None))
            for key in list(self.sessions):
                if len(self.sessions) <= self.session_limit:
                    break
                row = self.sessions[key]
                if key != session_id and row["sealed"] and row["accepted"] == row["completed"]:
                    del self.sessions[key]
            return result

    def flush(self, timeout):
        deadline = time.monotonic() + max(0.0, timeout)
        with self.condition:
            target = self.accepted
            while self.completed < target and not self.finished.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.condition.wait(min(remaining, math.sqrt(tick())))
            return FlushResult(ok=self.completed >= target and self.saved >= target and not self.failed and not self.rejected,
                               target=target, saved=self.saved, failed=self.failed, rejected=self.rejected, pending=max(0, target - self.completed),
                               retrying=self.retrying, lost_seconds=self.lost_seconds, error=self.failure)

    def close(self, timeout):
        deadline = time.monotonic() + max(0.0, timeout)
        self.closing.set()
        self.queue.close()
        result = self.flush(max(0.0, deadline - time.monotonic()))
        self.thread.join(timeout=max(0.0, deadline - time.monotonic()))
        if not result or self.thread.is_alive():
            self.emit("notice", key="storage_flush", **dict(result), writer_running=self.thread.is_alive())
        return bool(result) and not self.thread.is_alive()

    def status(self):
        with self.condition:
            budget = StorageBudget(self.model.state).snapshot(self.model.capacity() * self.model.np.dtype(self.model.np.float32).itemsize * 2, purpose="conversation")
            healthy = bool(self.storage_available and self.failure is None and not self.retrying and self.thread.is_alive() and not self.finished.is_set() and budget["ready"])
            return {"healthy": healthy, "budget": budget, "state": "ready" if healthy else "failed" if self.fatal else "closed" if self.closing.is_set() else "reconnecting",
                    "accepted": self.accepted, "saved": self.saved, "failed": self.failed, "rejected": self.rejected,
                    "pending": max(0, self.accepted - self.completed), "retrying": self.retrying, "retries": self.retries,
                    "lost_seconds": self.lost_seconds, "error": self.failure, "recoverable": self.fatal is None, "restarts": self.restarts}

class PublicAudioWorker:
    def __init__(self, state, model, emit, shutdown):
        self.state, self.model, self.emit, self.shutdown = Path(state), model, emit, shutdown
        self.closing = threading.Event()
        self.restarts = 0
        self.failure = None
        self.paused = threading.Event()
        self.paused.set()
        self.interrupt = threading.Event()
        self.wake = threading.Event()
        self.idle = threading.Event()
        self.idle.set()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.run, daemon=True, name="YuanPublicAudio")
        self.thread.start()

    def pause(self):
        with self.lock:
            self.paused.set()
            self.interrupt.set()
            self.wake.set()

    def resume(self):
        with self.lock:
            if not self.shutdown.is_set():
                self.interrupt.clear()
                self.paused.clear()
                self.wake.set()

    def wait_idle(self, cancel):
        timeout = self.model.policy.number("runtime", "source_pause_seconds", max(5.0, self.model.chunk_seconds * 2), 0.1, 3600.0)
        deadline = time.monotonic() + timeout
        while not self.idle.wait(min(math.sqrt(tick()), max(0.0, deadline - time.monotonic()))):
            check(cancel)
            if time.monotonic() >= deadline:
                raise EngineFault("联网任务未按时让出资源 / Network worker did not yield its resources in time")

    def run(self):
        store = sources = importer = owned_importer = None
        cancel = AnyCancel(self.shutdown, self.interrupt, self.closing)
        failures = 0
        base = self.model.policy.number("network", "worker_retry_seconds", max(0.1, self.model.chunk_seconds), tick(), 3600.0)
        ceiling = self.model.policy.number("network", "worker_retry_max_seconds", max(base, self.model.chunk_seconds * 8), base, 86400.0)
        try:
            while not self.shutdown.is_set() and not self.closing.is_set():
                with self.lock:
                    paused = self.paused.is_set()
                    if not paused:
                        self.idle.clear()
                if paused:
                    self.wake.wait(math.sqrt(tick()))
                    self.wake.clear()
                    continue
                began = time.monotonic()
                worked = False
                try:
                    check(cancel)
                    if store is None:
                        store = AudioStore(self.state, recover=False)
                        sources = PublicAudio(self.state, store, self.model, self.emit)
                        importer = DatasetImporter(self.state, store, self.model, self.emit)
                    owned_root = getattr(self.model, "owned_root", None)
                    if owned_root is not None and (owned_importer is None or owned_importer.root != safe_path(owned_root, "datasets")):
                        owned_importer = DatasetImporter(owned_root, store, self.model, self.emit, namespace="owned")
                    budget = StorageBudget(self.state).snapshot(self.model.capacity() * self.model.np.dtype(self.model.np.float32).itemsize * 2)
                    if budget["ready"]:
                        worked = owned_importer.step(cancel) if owned_importer is not None else False
                        if not worked:
                            worked = importer.step(cancel)
                        check(cancel)
                        worked = sources.step(cancel) or worked
                    else:
                        self.emit("network", key="learning_storage_pause", budget=budget)
                    failures = 0
                    self.failure = None
                except Cancelled:
                    self.emit("network", key="source_paused")
                except Exception as exc:
                    failures += 1
                    self.failure = concise_error(exc)
                    self.emit("network", key="offline", component="public_worker", reason=self.failure, attempt=failures,
                              retry_seconds=min(ceiling, base * min(failures, math.sqrt(ceiling / base)) ** 2))
                    if store is not None:
                        try:
                            store.close()
                        except (OSError, sqlite3.Error):
                            pass
                    store = sources = importer = owned_importer = None
                finally:
                    self.idle.set()
                measured = max(tick(), time.monotonic() - began)
                delay = min(ceiling, base * min(failures, math.sqrt(ceiling / base)) ** 2) if failures else max(math.sqrt(self.model.chunk_seconds * measured), self.model.chunk_seconds if not worked else math.sqrt(tick()))
                cancel.wait(delay)
        finally:
            self.idle.set()
            if store is not None:
                try:
                    store.close()
                except (OSError, sqlite3.Error) as exc:
                    self.emit("notice", key="history_full", reason=concise_error(exc))

    def close(self, timeout):
        self.closing.set()
        self.pause()
        self.thread.join(timeout=max(0.0, timeout))
        return not self.thread.is_alive()


    def ensure_running(self):
        with self.lock:
            if self.shutdown.is_set() or self.closing.is_set() or self.thread.is_alive():
                return
            self.restarts += 1
            self.thread = threading.Thread(target=self.run, daemon=True, name="YuanPublicAudio")
            self.thread.start()
        self.emit("network", key="retry", component="public_worker_restart", restarts=self.restarts)

class CaptureSession:
    def __init__(self, devices, session_id, parent, emit, writer):
        self.writer = writer
        self.devices, self.model, self.session_id, self.parent, self.emit = devices, devices.model, session_id, parent, emit
        self.stop_event = threading.Event()
        self.wake = threading.Event()
        self.connected = False
        self.speaking = False
        self.reply_epoch = 0
        self.utterance_id = None
        self.chunk_index = 0
        self.saved = self.consumed = 0
        self.packet_sequence = 0
        self.inference_dropped = 0
        self.failure = None
        self.inference = AudioPacketQueue(self.model.capacity() * self.model.np.dtype(self.model.np.float32).itemsize,
                                         max(1, math.isqrt(self.model.capacity() // self.model.hop)))
        self.thread = threading.Thread(target=self.run, daemon=True, name="YuanCapture")
        self.devices.capture = self

    def start(self):
        self.thread.start()
        return self

    def is_set(self):
        return self.parent.is_set() or self.stop_event.is_set()

    def wait(self, timeout):
        return self.stop_event.wait(timeout) or self.parent.is_set()

    def next(self, store=None):
        while not self.is_set():
            self.writer.ensure_running()
            packet = self.inference.get(math.sqrt(tick()))
            if packet is not None:
                self.consumed += 1
                return packet
            if not self.thread.is_alive():
                if self.failure is not None:
                    raise self.failure
                raise YuanError("收音任务已停止 / Audio capture worker stopped")
        raise Cancelled()

    def submit(self, processed, created, metadata):
        self.packet_sequence += 1
        processed.setflags(write=False)
        packet = {"id": uuid.uuid4().hex, "group_id": self.session_id, "role": "user", "waveform": processed, "rate": self.model.rate,
                  "count": len(processed), "created": created, "metadata": dict(metadata), "eligible": not metadata.get("playback_overlap") and not metadata.get("capture_gap"),
                  "sequence": self.packet_sequence}
        packet["storage_queued"] = self.writer.enqueue(packet)
        self.saved += int(packet["storage_queued"])
        if not metadata.get("final") and not self.is_set():
            accepted, dropped = self.inference.put(packet, replace_oldest=True, preserve_utterance=True)
            self.inference_dropped += len(dropped) + int(not accepted)
            if dropped or not accepted:
                self.emit("notice", key="inference_superseded" if accepted else "input_incomplete", id=self.session_id,
                          utterance_id=metadata["utterance_id"], samples=[item["id"] for item in dropped],
                          rejected=None if accepted else packet["id"], dropped=self.inference_dropped, whole_utterance=True)
        self.wake.set()
        return packet

    def stop(self):
        self.stop_event.set()
        self.devices.cancel_playback()
        self.wake.set()
        self.inference.close()
        timeout = self.devices.timeout("capture_stop", max(10.0, self.model.chunk_seconds * 4))
        with operation(self.emit, "capture_stop", scope="audio", budget_seconds=timeout):
            self.thread.join(timeout=timeout)
            if self.thread.is_alive():
                self.failure = self.devices.driver.poison("收音结束超时，尾部音频未确认 / Capture stop timed out; trailing audio unconfirmed")
                self.emit("notice", key="capture_incomplete", id=self.session_id, reason=concise_error(self.failure))
                raise self.failure
            if self.devices.capture is self:
                self.devices.capture = None
            if self.failure is not None:
                raise self.failure
        return True

    def run(self):
        try:
            while not self.is_set():
                try:
                    self.devices.open_streams(self)
                    self.connected = True
                    self.emit("status", key="listening", id=self.session_id)
                    self.emit("audio", id=self.session_id, continuous=True)
                    self.emit("notice", key="echo_note")
                    self.collect()
                except Cancelled:
                    break
                except EngineFault:
                    raise
                except Exception as exc:
                    self.emit("status", key="capture_recover", id=self.session_id)
                    self.emit("notice", key="capture_recover", reason=concise_error(exc))
                finally:
                    self.connected = self.speaking = False
                    self.emit("audio", id=self.session_id, continuous=False, speech_active=False, level=0.0)
                    self.reply_epoch += 1
                    self.devices.abort()
                    self.devices.close_streams()
                    self.wake.set()
                if not self.is_set():
                    self.wait(self.devices.policy.reconnect_seconds if self.devices.policy else math.sqrt(self.model.chunk_seconds))
        except Exception as exc:
            self.failure = exc
            self.emit("notice", key="capture_incomplete", reason=concise_error(exc), id=self.session_id)
        finally:
            self.connected = self.speaking = False
            self.inference.close()
            self.emit("audio", id=self.session_id, continuous=False, level=0.0, recorded=0.0)
            self.emit("capture_stopped", key="capture_stopped" if self.failure is None else "capture_incomplete", id=self.session_id, tail_complete=self.failure is None)
            self.wake.set()

    def collect(self):
        import numpy as np
        devices, model, policy = self.devices, self.model, self.devices.policy
        resampler = devices.soxr.ResampleStream(devices.input_rate, model.rate, 1, dtype="float32")
        gate = SpeechGate(model.rate, policy.frame_size, model.chunk_seconds, policy.segment_seconds, policy.pause_seconds, policy.onset_seconds)
        echo = EchoSuppressor(model.rate, policy)
        pending = np.empty(0, dtype=np.float32)
        origin = None
        position = 0
        prior_output_faults = devices.output_faults
        overlap_until = -math.inf
        segment_overlap = False
        segment_gap = False
        last_levels = 0.0
        segment_start = None
        def save(values, timestamp, final=False):
            nonlocal segment_overlap, segment_gap, segment_start
            if values is None or not len(values):
                return
            processed = model.normalize(values, model.rate)
            created = time.time() + timestamp - time.monotonic()
            metadata = {"reply_epoch": self.reply_epoch, "utterance_id": self.utterance_id, "chunk_index": self.chunk_index,
                        "utterance_final": None if final else not gate.active, "playback_overlap": segment_overlap, "capture_gap": segment_gap, "limited": gate.limited, "final": final}
            self.submit(processed, created, metadata)
            self.chunk_index += 1
            segment_overlap = segment_gap = False
            segment_start = None
        def consume(converted, final=False, interrupted=False):
            nonlocal pending, position, overlap_until, segment_overlap, segment_gap, segment_start, last_levels
            pending = np.concatenate((pending, converted))
            while len(pending) >= policy.frame_size or final and len(pending):
                take = min(len(pending), policy.frame_size)
                frame, pending = pending[:take], pending[take:]
                timestamp = origin + position / model.rate
                position += take
                if not np.isfinite(frame).all():
                    devices.input_faults += 1
                    devices.faults += 1
                    segment_gap = True
                    continue
                frame, overlap = echo.process(frame, timestamp, devices.reference)
                if overlap:
                    overlap_until = timestamp + gate.preroll.maxlen * gate.frame_seconds
                before = gate.active
                result = gate.push(frame)
                if not before and (gate.active or result is not None):
                    self.reply_epoch += 1
                    self.utterance_id = uuid.uuid4().hex
                    self.chunk_index = 0
                    segment_start = timestamp + take / model.rate - (sum(len(value) for value in gate.frames) if result is None else len(result)) / model.rate
                if before or gate.active or result is not None:
                    segment_overlap = segment_overlap or timestamp <= overlap_until
                self.speaking = gate.active
                if result is not None:
                    save(result, segment_start if segment_start is not None else timestamp - len(result) / model.rate, final=interrupted)
                    if gate.active:
                        segment_start = timestamp + take / model.rate
                now = time.monotonic()
                if now - last_levels >= math.sqrt(gate.frame_seconds):
                    last_levels = now
                    self.emit("audio", id=self.session_id, level=float(np.sqrt(np.mean(frame.astype(np.float64) ** 2))), probability=gate.confidence, faults=devices.faults + devices.ring.overflows,
                              input_faults=devices.input_faults + devices.ring.overflows, output_faults=devices.output_faults, recorded=sum(len(value) for value in gate.frames) / model.rate, continuous=True, speech_active=gate.active,
                              pending=self.inference.pending(), pending_storage=self.writer.queue.pending(), dropped_samples=devices.ring.dropped, echo_correlation=echo.correlation, echo_reduction=echo.reduction, playback_overlap=overlap)
        def ingest(raw, timestamp, gap):
            nonlocal resampler, gate, echo, pending, origin, position, segment_gap, segment_overlap, segment_start, overlap_until
            if gap:
                segment_gap = True
                if origin is not None:
                    consume(resampler.resample_chunk(np.empty(0, dtype=np.float32), last=True), final=True, interrupted=True)
                    save(gate.flush(), segment_start if segment_start is not None else origin + position / model.rate, final=True)
                resampler = devices.soxr.ResampleStream(devices.input_rate, model.rate, 1, dtype="float32")
                gate = SpeechGate(model.rate, policy.frame_size, model.chunk_seconds, policy.segment_seconds, policy.pause_seconds, policy.onset_seconds)
                echo = EchoSuppressor(model.rate, policy)
                pending = np.empty(0, dtype=np.float32)
                origin = None
                position = 0
                segment_gap = True
                segment_overlap = False
                segment_start = None
                overlap_until = -math.inf
                self.speaking = False
                self.reply_epoch += 1
                self.emit("notice", key="capture_gap", faults=devices.input_faults + devices.ring.overflows, dropped_samples=devices.ring.dropped, discontinuities=devices.ring.discontinuities, timestamp=timestamp)
            if origin is None:
                origin = timestamp
            consume(resampler.resample_chunk(raw))
        try:
            while not self.is_set():
                if devices.output_faults != prior_output_faults:
                    prior_output_faults = devices.output_faults
                    self.emit("notice", key="playback_gap", id=self.session_id, output_faults=prior_output_faults)
                raw, timestamp, gap = devices.ring.read_block(max(1, math.ceil(policy.frame_size * devices.input_rate / model.rate)))
                if raw.size:
                    ingest(raw, timestamp, gap)
                else:
                    if not devices.active():
                        raise YuanError("收音流已中断 / Capture stream was interrupted")
                    self.wait(policy.frame_size / model.rate)
        finally:
            devices.abort()
            if devices.driver.failure is not None:
                raise devices.driver.failure
            while devices.ring.count:
                raw, timestamp, gap = devices.ring.read_block(max(1, math.ceil(policy.frame_size * devices.input_rate / model.rate)))
                if raw.size:
                    ingest(raw, timestamp, gap)
            if origin is not None:
                consume(resampler.resample_chunk(np.empty(0, dtype=np.float32), last=True), final=True, interrupted=True)
                save(gate.flush(), segment_start if segment_start is not None else origin + position / model.rate, final=True)
            self.speaking = False

def concise_error(exc):
    seen = set()
    values = []
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        value = str(exc).strip().replace("\x00", "")
        values.append(type(exc).__name__ + (": " + value if value else ""))
        exc = exc.__cause__ if exc.__cause__ is not None else (None if exc.__suppress_context__ else exc.__context__)
    return redact(" ← ".join(values))

class Engine:
    def __init__(self, state, emit, shutdown=None):
        self.state, self.emit = Path(state), emit
        self.shutdown = shutdown if shutdown is not None else threading.Event()
        self.interrupt = threading.Event()
        self.requests = queue.Queue()
        self.devices = None
        self.session_id = None
        self.language = "中文"
        self.store = None
        self.model = None
        self.last_metrics = 0.0
        self.metrics_period = math.sqrt(os.cpu_count() or 1)
        self.command_lock = threading.Lock()
        self.closed_sessions = set()
        self.cached_samples = {}
        self.resource_thread = None
        self.writer = self.sources = None
        self.runtime_ready = self.pipeline_ready = self.audio_ready = False
        self.learner_ready = False
        self.next_audio_probe = 0.0
        self.audio_signature = self.audio_failure = self.last_readiness = None

    def receive(self):
        try:
            while not self.shutdown.is_set():
                line = sys.stdin.readline()
                if not line:
                    break
                try:
                    action = json.loads(line)
                except (ValueError, TypeError):
                    continue
                if not isinstance(action, dict):
                    continue
                command = action.get("action")
                if command not in ("start", "stop", "close", "language", "import_pair", "review_open", "review_play", "review_submit", "review_close", "export_release"):
                    continue
                if command in ("import_pair", "review_open", "review_play", "review_submit", "review_close", "export_release") and self.session_id is not None:
                    self.emit("notice", key="learn_yield")
                    continue
                with self.command_lock:
                    if command == "language":
                        self.language = "English" if action.get("language") == "English" else "中文"
                        continue
                    if command == "stop":
                        self.closed_sessions.add(str(action.get("id", "")))
                    self.interrupt.set()
                    if self.sources:
                        self.sources.pause()
                    if command == "close":
                        self.shutdown.set()
                    if self.devices:
                        self.devices.cancel_playback()
                    try:
                        self.requests.put_nowait(action)
                    except queue.Full:
                        self.shutdown.set()
        finally:
            self.shutdown.set()
            self.interrupt.set()
            if self.devices:
                self.devices.cancel_playback()

    def stats(self, force=False):
        import psutil
        now = time.monotonic()
        if threading.current_thread() is threading.main_thread() and self.store:
            self.cached_samples = self.store.stats()
        if not force and now - self.last_metrics < self.metrics_period:
            return
        self.last_metrics = now
        memory = psutil.virtual_memory()
        limits = available_resources(psutil)
        disk = shutil.disk_usage(self.state)
        process = psutil.Process()
        values = {"cpu": psutil.cpu_percent(), "cpu_limit": limits["cpus"], "ram_used": memory.used, "ram_available": limits["memory"], "process_ram": process.memory_info().rss,
                  "disk_free": disk.free, "disk_total": disk.total, "samples": self.cached_samples,
                  "storage_budget": getattr(self, "storage_budget", {}),
                  "capture": {"id": None, "continuous": False, "speech_active": False, "pending": 0}}
        if self.devices and self.devices.capture:
            capture = self.devices.capture
            values["capture"] = {"id": capture.session_id, "continuous": capture.connected, "speech_active": capture.speaking, "pending": capture.inference.pending(), "pending_storage": capture.writer.queue.pending()}
        if self.devices:
            values["audio_clocks"] = {key: {"source": "hardware" if row["hardware"] else "sample_count", "arrival": row["arrival"]}
                                      for key, row in getattr(self.devices, "clock_states", {}).copy().items()}
        if self.writer:
            values["storage"] = self.writer.status()
        if self.model and self.model.device.type == "cuda":
            try:
                free, total = self.model.torch.cuda.mem_get_info()
                values.update(gpu_used=total - free, gpu_total=total)
            except RuntimeError:
                pass
        self.emit("metrics", **values)

    def resource_loop(self):
        failure = None
        while not self.shutdown.wait(max(math.sqrt(tick()), min(1.0, self.metrics_period))):
            try:
                self.stats(True)
                if failure:
                    self.emit("notice", key="recovered", component="resources")
                failure = None
            except Exception as exc:
                reason = concise_error(exc)
                if reason != failure:
                    self.emit("notice", key="resource_failure", reason=reason)
                failure = reason

    @supervised('memory_recall', 'conversation')
    def _memory_context(self, waveform, session_id, cancel):
        np = self.model.np
        total = self.store.memory_count(session_id)
        budget = self.model.context_budget(len(waveform))
        if not total or budget < self.model.hop:
            return []
        pool = self.model.policy.integer("memory", "candidate_pool", max(1, math.isqrt(total)), 1, max(1, total))
        batch = self.model.policy.integer("memory", "search_batch", max(1, math.isqrt(total)), 1, max(1, total))
        recall = self.model.policy.integer("memory", "recall_count", max(1, math.isqrt(pool)), 1, pool)
        query = self.model.memory_vector(waveform, "user", cancel)
        query_norm = float(np.linalg.norm(query))
        if not math.isfinite(query_norm) or query_norm <= np.finfo(np.float32).eps:
            return []
        key = self.model.serving_digest or self.model.active_digest or self.model.identity()
        scored = []
        scanned = 0
        for row in self.store.memory_rows(session_id, key, batch, cancel):
            check(cancel)
            scanned += 1
            try:
                dimensions = int(row["dims"])
                if dimensions != len(query) or len(row["vector"]) != dimensions * np.dtype(np.float32).itemsize:
                    continue
                vector = np.frombuffer(row["vector"], dtype=np.float32, count=dimensions)
                if not np.isfinite(vector).all():
                    continue
                denominator = query_norm * float(np.linalg.norm(vector))
                if not math.isfinite(denominator) or denominator <= np.finfo(np.float32).eps:
                    continue
                similarity = float(np.dot(query, vector) / denominator)
                if not math.isfinite(similarity) or similarity <= 0:
                    continue
                item = (similarity, row["id"], row)
                if len(scored) < pool:
                    heapq.heappush(scored, item)
                elif item[:2] > scored[0][:2]:
                    heapq.heapreplace(scored, item)
            except (ValueError, TypeError, OverflowError):
                continue
        retained = []
        for similarity, identifier, row in sorted(scored, key=lambda item: (-item[0], item[1])):
            check(cancel)
            if len(retained) >= recall or budget < self.model.hop:
                break
            count = min(int(row["count"]), int(min(self.model.window(), budget) * int(row["rate"]) / self.model.rate))
            if count <= 0:
                continue
            try:
                values = self.store.read_audio(row, row["count"] - count, count, cancel=cancel)
                if row["rate"] != self.model.rate:
                    values = self.model.normalize(values, int(row["rate"]))
                values = values[-budget:]
                if len(values) < self.model.hop:
                    continue
                retained.append((row["created"], row["id"], "assistant" if row["role"] == "assistant" else "user", values, similarity))
                budget -= len(values)
            except Cancelled:
                raise
            except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                self.emit("notice", key="sample_skipped", component="memory_recall", sample=row["id"], reason=concise_error(exc), degraded=True)
        retained.sort(key=lambda item: (item[0], item[1]))
        if retained:
            self.emit("memory_matches", key="memory_recall", id=session_id, count=len(retained), samples=sum(len(item[3]) for item in retained),
                      best=max(item[4] for item in retained), scanned=scanned, context_selected=False)
        return [(item[2], item[3]) for item in retained]

    @supervised('memory_encoding', 'learning')
    def index_memory(self, cancel):
        if not self.model.playback_ready:
            return False
        key = self.model.serving_digest or self.model.active_digest or self.model.identity()
        self.store.set_setting("serving_memory_key", key)
        row = self.store.db.execute("SELECT s.* FROM samples s JOIN groups g ON g.id=s.group_id JOIN sessions x ON x.id=s.group_id LEFT JOIN memories m ON m.sample_id=s.id AND m.model_key=? WHERE g.origin='local' AND x.ended IS NOT NULL AND s.audio_status='ready' AND s.memory_eligible=1 AND s.count>1 AND m.sample_id IS NULL ORDER BY s.created DESC LIMIT 1", (key,)).fetchone()
        if row is None:
            return False
        check(cancel)
        take = min(row["count"], max(1, int(self.model.window() * row["rate"] / self.model.rate)))
        try:
            values = self.store.read_audio(row, row["count"] - take, take, cancel=cancel)
            if row["rate"] != self.model.rate:
                values = self.model.normalize(values, int(row["rate"]))
            vector = self.model.memory_vector(values, "assistant" if row["role"] == "assistant" else "user", cancel)
            self.store.save_memory_vector(row["id"], key, vector)
        except Cancelled:
            raise
        except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
            self.store.audio_failure(row, exc)
        return True

    def check_pipeline(self):
        import numpy as np
        count = self.model.hop * max(4, self.model.layers + 1)
        generator = np.random.default_rng(int(self.model.identity()[:16], 16))
        amplitude = np.finfo(np.float32).eps ** 0.25
        waveform = generator.normal(0.0, amplitude, count).astype(np.float32)
        output = self.model.respond_audio(tr("default_prompt", self.language), [], waveform, self.shutdown, max_samples=count)
        if not output.size or not np.isfinite(output).all():
            raise YuanError("端到端音频链路验证失败 / End-to-end audio pipeline validation failed")
        self.emit("pipeline", verified=True, conversation_verified=self.model.dialogue_ready, path="ordered prompt + processed audio → Yuan audio encoder → latent dialogue core → Yuan audio decoder → autoregressive waveform",
                  transcription=False, text_to_speech=False, external_model=False, playback=self.model.playback_ready)

    def readiness(self, force=False):
        audio_ready = bool(self.audio_ready and self.devices is not None and self.devices.driver.failure is None)
        values = {"runtime_ready": self.runtime_ready, "pipeline_ready": self.pipeline_ready, "audio_ready": audio_ready,
                  "model_ready": bool(self.model and self.model.playback_ready and self.model.dialogue_ready and self.model.serving_network is not None),
                  "storage_ready": bool(self.writer and self.writer.status()["healthy"])}
        values["learning_ready"] = bool(self.runtime_ready and self.pipeline_ready and self.model is not None and self.store is not None and self.learner_ready)
        values["conversation_ready"] = can_converse(values) and values["storage_ready"]
        values["blockers"] = [key for key in ("runtime_ready", "pipeline_ready", "audio_ready", "model_ready", "storage_ready") if not values[key]]
        values["capability"] = self.model.capability() if self.model else "cold_start"
        values["learning"] = self.learning_state()
        values["model_blocker"] = None if values["model_ready"] else "release_invalid" if self.model and self.model.release_failure else "release_wait" if self.model and self.model.dialogue_trained else "native_learning"
        missing = values["learning"].get("missing", [])
        if values["model_blocker"] == "native_learning" and any(row.get("split") == "train" for row in missing):
            values["model_blocker"] = "dialogue_data_missing"
        values["model_detail"] = self.model.release_failure if self.model else None
        values["delivery_source"] = str(self.model.owned_root) if self.model and self.model.owned_root else None
        values["model_requires_action"] = not values["model_ready"] and values["model_blocker"] in ("release_invalid", "release_wait")
        values["automatic_learning"] = values["learning_ready"]
        values["serving_model"] = self.model.serving_digest if self.model else None
        values["learning_model"] = self.model.active_digest if self.model else None
        if force or values != self.last_readiness:
            self.last_readiness = dict(values)
            self.emit("readiness", **values)
        return values

    def refresh_delivery(self, cancel):
        candidates = (self.state.parent / "Yuan.release.json", Path(__file__).resolve().with_name("Yuan.release.json"))
        descriptor = next((path for path in candidates if path.is_file() and not path.is_symlink()), None)
        if descriptor is None:
            return False
        marker = digest_file(descriptor, cancel)
        now = time.monotonic()
        if marker == getattr(self, "delivery_marker", None):
            return False
        if marker == getattr(self, "delivery_failure_marker", None) and now < getattr(self, "delivery_retry_at", 0.0):
            return False
        current = read_json(safe_path(self.state, "owned-release.json"), {})
        source = checked_json(descriptor)
        if source.get("sha256") == current.get("bundle_sha256"):
            self.delivery_marker = marker
            return False
        self.sources.pause()
        self.sources.wait_idle(cancel)
        fields = ("owned_root", "serving_network", "serving_digest", "serving_manifest", "release_root", "dialogue_ready", "playback_ready",
                  "release_signature", "release_files", "release_cases", "release_failure")
        saved = {key: getattr(self.model, key) for key in fields if hasattr(self.model, key)}
        committed = False
        try:
            installer = OwnedReleaseInstaller(self.state, self.emit)
            root = installer.prepare(cancel)
            if root is None:
                return False
            supplied = checked_json(safe_path(root, "learning/model.json"))
            if any(supplied.get(key) != getattr(self.model, key) for key in ("rate", "hop", "width", "layers")):
                self.delivery_marker = marker
                self.emit("notice", key="release_restart_required")
                return False
            self.model.owned_root = root
            if not self.model._load_review(safe_path(root, "learning/release.json"), state=root, announce=False):
                raise YuanError("新交付未通过独立验收 / New delivery did not pass independent acceptance")
            installer.activate(root, self.model, cancel)
            committed = True
            self.delivery_marker = marker
            self.delivery_failures = 0
            self.delivery_retry_at = 0.0
            self.model.record_serving()
            local_release = safe_path(self.state, "learning/release.json")
            if local_release.exists():
                atomic_json(safe_path(self.state, "learning/previous-release.json"), checked_json(local_release), cancel)
                local_release.unlink()
            learner = getattr(self, "learner", None)
            if learner is not None:
                learner._discard_candidate()
            self.model.network = copy.deepcopy(self.model.serving_network).eval().requires_grad_(False)
            self.model.steps = int(self.model.serving_manifest["steps"])
            self.model.accepted = int(self.model.serving_manifest.get("accepted", 0))
            self.model.training_manifest = self.model.serving_manifest
            self.model._persist(self.model.network, self.model.serving_manifest.get("metrics", {}), cancel, increment=False, task="dialogue")
            return True
        except Cancelled:
            if not committed:
                for key, value in saved.items():
                    setattr(self.model, key, value)
            raise
        except Exception as exc:
            if not committed:
                for key, value in saved.items():
                    setattr(self.model, key, value)
                self.delivery_failure_marker = marker
                self.delivery_failures = getattr(self, "delivery_failures", 0) + 1
                self.delivery_retry_at = now + self.store.retry_delay(self.delivery_failures)
            self.emit("notice", key="release_invalid" if not committed else "checkpoint_failure", candidate=not committed,
                      serving_preserved=bool(self.model.playback_ready), reason=concise_error(exc))
            return committed

    def probe_audio(self, cancel, force=False):
        now = time.monotonic()
        interval = self.model.policy.number("audio", "probe_interval_seconds", max(5.0, self.model.chunk_seconds * 2), 1.0, 3600.0)
        if not force and now < self.next_audio_probe:
            return self.audio_ready
        self.next_audio_probe = now + interval
        try:
            indices, devices = self.devices.probe()
            signature = [(index, device["name"], device["default_samplerate"], device["selected_channels"]) for index, device in zip(indices, devices)]
            if force or not self.audio_ready or signature != self.audio_signature:
                self.devices.preflight(cancel)
            self.audio_signature = signature
            self.audio_ready = True
            self.audio_failure = None
        except (Cancelled, EngineFault):
            raise
        except Exception as exc:
            self.audio_ready = False
            reason = concise_error(exc)
            if reason != self.audio_failure:
                self.emit("notice", key="mic_missing", reason=reason)
            self.audio_failure = reason
        return self.audio_ready

    def conversation(self, action):
        session_id = str(action.get("id", ""))
        if not session_id or session_id in self.closed_sessions:
            self.emit("ended", id=session_id)
            return
        if not self.readiness()["conversation_ready"]:
            self.emit("notice", key="start_rejected", id=session_id, blockers=self.readiness()["blockers"])
            self.emit("ended", id=session_id)
            return
        self.model.review_held = None
        self.language = "English" if action.get("language") == "English" else "中文"
        prompt = str(action.get("prompt", "")).strip() or tr("default_prompt", self.language)
        history = deque()
        utterance = None
        def retain(role, values, metadata=None):
            if metadata and AudioStore.memory_reason(metadata, generated=role == "assistant"):
                return
            history.append((role, values))
            budget = self.model.capacity() // 2
            total = sum(len(item[1]) for item in history)
            removed = max(0, total - budget)
            while total > budget and history:
                old_role, old_values = history.popleft()
                take = min(len(old_values), total - budget)
                total -= take
                if take < len(old_values):
                    history.appendleft((old_role, old_values[take:]))
            if removed:
                self.emit("context_budget", key="context_trimmed", id=session_id, kept_samples=total, dropped_samples=removed, budget_samples=budget)
        turns = 0
        started = time.monotonic()
        capture = None
        registered = False
        try:
            if self.sources:
                self.sources.pause()
                self.sources.wait_idle(self.interrupt)
            self.model.refresh_release()
            self.probe_audio(self.interrupt, force=True)
            if not self.readiness()["conversation_ready"]:
                self.emit("notice", key="start_rejected", id=session_id, blockers=self.readiness()["blockers"])
                return
            check(self.interrupt)
            self.store.session(session_id, prompt, self.language)
            self.store.group(session_id, "local")
            registered = True
            self.session_id = session_id
            self.model.input_budget = max(self.model.hop * 2, self.model.capacity() // 2)
            capture = CaptureSession(self.devices, session_id, self.interrupt, self.emit, self.writer)
            self.emit("started", id=session_id, mode="dialogue", model=self.model.serving_digest, continuous=False,
                      acoustic_ready=self.model.acoustic_ready, dialogue_ready=True, playback_ready=True)
            capture.start()
            while not self.shutdown.is_set():
                check(self.interrupt)
                row = capture.next()
                processed = row["waveform"]
                if row["rate"] != self.model.rate or processed.ndim != 1 or len(processed) != row["count"] or not self.model.np.isfinite(processed).all():
                    self.emit("notice", key="capture_gap", sample=row["id"], reason="音频内存片段无效 / Invalid in-memory audio segment")
                    continue
                metadata = row["metadata"]
                epoch = int(metadata["reply_epoch"])
                input_cancel = ReplyCancel(self.interrupt, capture, epoch, allow_speaking=True)
                identifier = metadata["utterance_id"]
                if input_cancel.is_set():
                    retain("user", processed, metadata)
                    self.emit("input_state", key="inference_superseded", id=session_id, utterance_id=identifier, chunk_index=metadata["chunk_index"])
                    continue
                if utterance is None or utterance["id"] != identifier:
                    utterance = {"id": identifier, "hidden": None, "next": 0, "samples": 0, "energy": 0.0, "incomplete": False, "started": time.monotonic()}
                utterance["incomplete"] = utterance["incomplete"] or metadata["chunk_index"] != utterance["next"] or metadata.get("capture_gap", False)
                utterance["next"] = metadata["chunk_index"] + 1
                utterance["samples"] += len(processed)
                utterance["energy"] += float(self.model.np.dot(processed.astype(self.model.np.float64), processed.astype(self.model.np.float64)))
                if not utterance["incomplete"]:
                    try:
                        recalled = []
                        if utterance["hidden"] is None:
                            try:
                                recalled = self._memory_context(processed, session_id, input_cancel)
                            except Cancelled:
                                raise
                            except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                                self.emit("notice", component="memory_recall", reason=concise_error(exc), degraded=True)
                        utterance["hidden"] = self.model.encode_input(prompt, list(history), processed, input_cancel, hidden=utterance["hidden"],
                                                                     final=metadata["utterance_final"], recalled=recalled, context_id=session_id)
                    except Cancelled:
                        retain("user", processed, metadata)
                        continue
                    except Exception as exc:
                        utterance["incomplete"] = True
                        self.emit("notice", key="input_incomplete", component="utterance_encoder", reason=concise_error(exc), id=session_id, utterance_id=identifier)
                self.emit("input_state", id=session_id, utterance_id=identifier, chunk_index=metadata["chunk_index"], utterance_final=metadata["utterance_final"],
                          input_samples=utterance["samples"], incomplete=utterance["incomplete"], reply_epoch=epoch)
                if not metadata["utterance_final"]:
                    retain("user", processed, metadata)
                    continue
                if utterance["incomplete"]:
                    retain("user", processed, metadata)
                    self.emit("notice", key="input_incomplete", id=session_id, utterance_id=identifier, input_samples=utterance["samples"])
                    utterance = None
                    continue
                reply_cancel = ReplyCancel(self.interrupt, capture, epoch)
                began = time.monotonic()
                heard = None
                output_sample = None
                latency = None
                mode = "superseded"
                playback_started = None
                playback_quality = {}
                try:
                    if not reply_cancel.is_set():
                        mode = "dialogue"
                        self.emit("status", key="thinking", id=session_id)
                        output = self.model.stream_audio(prompt, (), processed, reply_cancel, hidden=utterance["hidden"],
                                                         reference_rms=math.sqrt(utterance["energy"] / max(1, utterance["samples"])))
                        playback_started = time.time()
                        result = self.devices.play_stream(output, self.model.output_rate, reply_cancel)
                        heard, interrupted = result.waveform, result.interrupted
                        playback_quality = result.metadata()
                        actual_start = result.started_at
                        if actual_start is not None:
                            latency = max(0.0, actual_start - began)
                            playback_started = time.time() + actual_start - time.monotonic()
                        if result.error is not None:
                            mode = "failed"
                        elif interrupted:
                            mode = "interrupted"
                            self.emit("notice", key="barge_in", sample=row["id"])
                except Cancelled:
                    mode = "interrupted"
                    if not self.interrupt.is_set():
                        self.emit("notice", key="barge_in", sample=row["id"])
                except Exception as exc:
                    mode = "failed"
                    self.emit("notice", reason=concise_error(exc), component="reply", input_sample=row["id"])
                    if isinstance(exc, MemoryError) or "out of memory" in str(exc).lower():
                        self.model.capacity_scale /= math.sqrt(2)
                        self.model.input_budget = max(self.model.hop * 2, self.model.capacity() // 2)
                        import gc
                        gc.collect()
                        if self.model.torch.cuda.is_available():
                            self.model.torch.cuda.empty_cache()
                finally:
                    retain("user", processed, metadata)
                    if heard is not None and len(heard):
                        heard = self.model.np.ascontiguousarray(heard, dtype=self.model.np.float32)
                        heard.setflags(write=False)
                        playback_quality["interrupted"] = bool(playback_quality.get("interrupted") or mode in ("interrupted", "failed"))
                        retain("assistant", heard, playback_quality)
                        output_sample = uuid.uuid4().hex
                        packet = {"id": output_sample, "group_id": session_id, "role": "assistant", "waveform": heard,
                                  "rate": self.model.output_rate, "created": playback_started, "eligible": False,
                                  "metadata": {"input_sample": row["id"], "input_utterance": identifier, "input_chunks": utterance["next"], "model": self.model.serving_digest, **playback_quality}}
                        if not self.writer.enqueue(packet):
                            output_sample = None
                    turns += 1
                    self.emit("turn", id=session_id, turns=turns, latency=latency, input_seconds=utterance["samples"] / self.model.rate, utterance_id=identifier, input_chunks=utterance["next"],
                              output_seconds=len(heard) / self.model.output_rate if heard is not None else 0.0, elapsed=time.monotonic() - started,
                              mode=mode, input_sample=row["id"], output_sample=output_sample, model=self.model.serving_digest, playback=playback_quality,
                              pending=capture.inference.pending(), pending_storage=self.writer.queue.pending(), continuous=capture.connected)
                    utterance = None
                    if capture.connected and not self.interrupt.is_set():
                        self.emit("status", key="listening", id=session_id)
                    self.model.policy.save()
                    self.stats(True)
        except Cancelled:
            pass
        except EngineFault:
            raise
        except Exception as exc:
            if not self.interrupt.is_set():
                self.emit("notice", reason=concise_error(exc), component="conversation")
        finally:
            cleanup_failure = self.devices.driver.failure
            try:
                if capture is not None:
                    capture.stop()
                self.devices.abort()
                self.devices.close_streams()
            except Exception as exc:
                cleanup_failure = exc
                self.emit("notice", key="capture_incomplete", id=session_id, reason=concise_error(exc))
            timeout = self.model.policy.number("audio", "storage_flush_seconds", max(5.0, self.model.chunk_seconds * 2), 0.1, 3600.0)
            result = FlushResult(ok=False, pending=0)
            if self.writer:
                result = self.writer.flush_session(session_id, timeout)
                self.emit("storage_state", **self.writer.status())
                if not result:
                    self.emit("notice", key="storage_flush", **dict(result))
            if registered:
                try:
                    self.store.end_session(session_id)
                except (OSError, sqlite3.Error) as exc:
                    self.emit("notice", reason=concise_error(exc))
            self.model.input_budget = None
            history.clear()
            self.session_id = None
            complete = bool(result) and cleanup_failure is None
            self.emit("ended", id=session_id, turns=turns, storage_complete=complete, capture_complete=cleanup_failure is None, storage=dict(result))
            self.readiness(True)
            self.stats(True)
            if cleanup_failure is not None:
                raise EngineFault("会话结束未完成，重启音频引擎 / Session cleanup incomplete; restarting audio engine") from cleanup_failure

    def run(self):
        import gc
        receiver = threading.Thread(target=self.receive, daemon=True, name="YuanControl")
        receiver.start()
        model = None
        learner = None
        try:
            owned_release_preflight(self.state, self.emit)
            dependencies = load_runtime_modules(self.state, self.emit, self.shutdown)
            psutil = dependencies["psutil"]
            self.runtime_ready = True
            with operation(self.emit, "stage_owned_release"):
                owned_root = OwnedReleaseInstaller(self.state, self.emit).prepare(self.shutdown)
            with operation(self.emit, "stage_store"):
                self.store = AudioStore(self.state)
            self.language = self.store.setting("language", "中文")
            settings = read_json(self.state / "settings.json")
            if settings.get("language") in ("中文", "English"):
                self.language = settings["language"]
            self.stats(True)
            self.resource_thread = threading.Thread(target=self.resource_loop, daemon=True)
            self.resource_thread.start()
            self.emit("status", key="loading")
            model = NativeAudio(self.state, self.shutdown, self.emit, owned_root=owned_root, dependencies=dependencies)
            self.model = model
            if owned_root is not None and model.playback_ready:
                OwnedReleaseInstaller(self.state, self.emit).activate(owned_root, model, self.shutdown)
            with operation(self.emit, "stage_audio"):
                self.devices = AudioDevices(model, self.emit)
            self.emit("status", key="checking")
            with operation(self.emit, "stage_pipeline"):
                try:
                    self.model.calibrate(self.shutdown)
                    self.check_pipeline()
                except RuntimeError as exc:
                    if model.device.type == "cpu":
                        raise
                    self.emit("notice", key="device_fallback", reason=concise_error(exc))
                    model.switch_device("cpu", self.shutdown)
                    self.check_pipeline()
            self.pipeline_ready = True
            with operation(self.emit, "stage_learning"):
                learner = AudioLearner(model, self.store, self.emit)
                self.learner = learner
                self.writer = AudioWriter(model, self.emit)
                self.sources = PublicAudioWorker(self.state, model, self.emit, self.shutdown)
                self.learner_ready = True
            self.probe_audio(self.shutdown, force=True)
            self.emit("ready", runtime_ready=True, capability=model.capability(), conversation_verified=model.dialogue_ready,
                      acoustic_ready=model.acoustic_ready, dialogue_trained=model.dialogue_trained, dialogue_ready=model.dialogue_ready, playback_ready=model.playback_ready)
            self.readiness(True)
            self.stats(True)
            duration = model.chunk_seconds
            failures = 0
            while not self.shutdown.is_set():
                self.writer.ensure_running()
                self.sources.ensure_running()
                action = None
                with self.command_lock:
                    try:
                        action = self.requests.get_nowait()
                    except queue.Empty:
                        pass
                    if self.requests.empty() and not self.shutdown.is_set():
                        self.interrupt.clear()
                if action:
                    if action["action"] == "close":
                        break
                    if action["action"] == "start":
                        if self.interrupt.is_set():
                            self.emit("ended", id=str(action.get("id", "")))
                        else:
                            self.conversation(action)
                        continue
                    if action["action"] == "stop":
                        self.closed_sessions.discard(str(action.get("id", "")))
                        self.emit("ended", id=str(action.get("id", "")))
                        continue
                    if action["action"] in ("import_pair", "review_open", "review_play", "review_submit", "review_close", "export_release"):
                        try:
                            self.sources.pause()
                            self.sources.wait_idle(self.interrupt)
                            self.learning_action(action, self.interrupt)
                        except Cancelled:
                            pass
                        except EngineFault:
                            raise
                        except Exception as exc:
                            self.emit("notice", key="dataset_rejected" if action["action"] == "import_pair" else "release_invalid", reason=concise_error(exc))
                        self.readiness(True)
                        continue
                began = time.monotonic()
                try:
                    learner._resume_activation(self.interrupt)
                    budget = StorageBudget(self.state)
                    required = model.capacity() * model.np.dtype(model.np.float32).itemsize * 2
                    space = budget.snapshot(required)
                    if not space["ready"]:
                        self.sources.pause()
                        self.sources.wait_idle(self.interrupt)
                        model._prune_models()
                        model._prune_evaluations(self.interrupt)
                        space = budget.reclaim(self.store, self.interrupt, required)
                    self.storage_budget = space
                    self.emit("storage_budget", **space)
                    if space["ready"]:
                        self.sources.resume()
                    else:
                        self.sources.pause()
                    self.store.set_setting("language", self.language)
                    recovered = self.store.recheck_audio(self.interrupt)
                    if recovered:
                        self.emit("notice", key="recovered", component="audio_integrity", samples=recovered)
                    self.refresh_delivery(self.interrupt)
                    model.refresh_release()
                    self.probe_audio(self.interrupt)
                    self.readiness()
                    try:
                        battery = psutil.sensors_battery()
                    except (AttributeError, OSError, NotImplementedError):
                        battery = None
                    worked = self.index_memory(self.interrupt) if space["ready"] else False
                    training_budget = model.training_budget(learner)
                    if not space["ready"]:
                        self.emit("learning", key="learning_storage_pause", budget=space, **learner._state())
                    elif not training_budget["ready"]:
                        fixed = training_budget["device_available"] < training_budget["minimum_bytes"]
                        self.emit("learning", key="memory_fixed_pause" if fixed else "memory_pause", budget=training_budget, **learner._state())
                    else:
                        try:
                            worked = learner.step(self.interrupt)
                        except Cancelled:
                            raise
                        except Exception as exc:
                            self.emit("learning", key="paused", reason=concise_error(exc), **learner._state())
                            if isinstance(exc, MemoryError) or "out of memory" in str(exc).lower():
                                model.recover_training_memory(learner)
                    check(self.interrupt)
                    if space["ready"]:
                        worked = model.prepare_review(self.store, self.interrupt) or worked
                    self.readiness()
                    self.stats(True)
                    measured = max(tick(), time.monotonic() - began)
                    duration = max(tick(), math.sqrt(duration * measured))
                    load = psutil.cpu_percent() / 100
                    delay = max(math.sqrt(model.chunk_seconds * measured), duration) * (1 + load) if worked else max(model.chunk_seconds, duration)
                    if battery and not battery.power_plugged:
                        delay *= 100 / max(1, battery.percent)
                    failures = 0
                    self.interrupt.wait(delay)
                except EngineFault:
                    raise
                except Cancelled:
                    self.emit("learning", key="learn_yield", **learner._state())
                except Exception as exc:
                    failures += 1
                    self.emit("learning", key="paused", reason=concise_error(exc), **learner._state())
                    if isinstance(exc, MemoryError) or "out of memory" in str(exc).lower():
                        model.recover_training_memory(learner)
                    gc.collect()
                    if model.torch.cuda.is_available():
                        model.torch.cuda.empty_cache()
                    self.interrupt.wait(max(model.chunk_seconds, duration, time.monotonic() - began) * math.sqrt(failures))
        except Cancelled:
            pass
        except BaseException as exc:
            self.emit("error", key="failure", reason=concise_error(exc), fatal=True, fault=fault_event(exc))
        finally:
            self.shutdown.set()
            self.interrupt.set()
            if self.devices:
                try:
                    self.devices.abort()
                    self.devices.close_streams()
                except Exception as exc:
                    self.emit("notice", key="capture_incomplete", reason=concise_error(exc))
                self.devices.driver.close(math.sqrt(tick()))
            timeout = model.policy.number("runtime", "shutdown_grace_seconds", max(5.0, model.chunk_seconds * 2), 1.0, 3600.0) if model else self.metrics_period
            if self.sources and not self.sources.close(timeout):
                self.emit("notice", key="source_paused", reason="联网任务正在退出 / Network worker is shutting down")
            if self.writer:
                self.writer.close(timeout)
            if self.resource_thread:
                self.resource_thread.join(timeout=max(1.0, self.metrics_period))
            if learner is not None:
                try:
                    learner.checkpoint(None, force=True)
                except Exception as exc:
                    self.emit("notice", key="checkpoint_failure", component="shutdown_checkpoint", reason=concise_error(exc))
            if self.store:
                try:
                    self.store.close()
                except (OSError, sqlite3.Error) as exc:
                    self.emit("notice", reason=concise_error(exc))
            self.emit("exit")

    def learning_state(self):
        if self.model is None or self.store is None:
            return {"missing": [], "sources": {}, "review_pending": False}
        minimum = AcceptancePolicy(self.model).minimum
        rows = self.store.db.execute("SELECT g.split,json_extract(p.evidence,'$.language') AS language,count(DISTINCT g.source_key) AS n FROM turn_pairs p JOIN groups g ON g.id=p.group_id JOIN samples u ON u.id=p.user_sample JOIN samples a ON a.id=p.assistant_sample WHERE p.verified=1 AND p.eligible=1 AND u.count>=? AND a.count>=? AND u.dialogue_eligible=1 AND a.dialogue_eligible=1 AND u.audio_status='ready' AND a.audio_status='ready' AND u.generated=0 AND a.generated=0 GROUP BY g.split,language", (self.model.hop, self.model.hop)).fetchall()
        sources = {language: {split: 0 for split in ("train", "validation", "guard", "release")} for language in ("中文", "English")}
        for row in rows:
            if row["language"] in sources:
                sources[row["language"]][row["split"]] = row["n"]
        missing = []
        for language, splits in sources.items():
            for split, count in splits.items():
                required = minimum if split == "release" else 1
                if count < required:
                    missing.append({"language": language, "split": split, "have": count, "need": required})
        pointer = read_json(safe_path(self.state, "learning/pending-review.json"))
        serving = read_json(safe_path(self.state, "serving-state.json"), {})
        return {"new_data": self.store.stats(), "candidate_improvements": self.model.accepted, "serving_versions": serving.get("versions", 0),
                "missing": missing, "sources": sources, "review_pending": bool(pointer.get("review_file")),
                "review_model": pointer.get("model_sha256"), "dialogue_trained": self.model.dialogue_trained,
                "serving_model": self.model.serving_digest, "active_model": self.model.active_digest,
                "candidate_isolated": bool(self.model.active_digest and self.model.active_digest != self.model.serving_digest)}

    @supervised('learning_action', 'learning')
    def learning_action(self, action, cancel):
        if os.environ.get("YUAN_PUBLISHER") != "1":
            raise YuanError("训练标注与人工验收仅供开发端使用 / Training annotation and manual acceptance are publisher-only operations")
        command = action["action"]
        if command == "export_release":
            OwnedReleaseInstaller(self.state, self.emit).export(self.model, cancel)
            return
        if command == "import_pair":
            return self.import_pair(action, cancel)
        if command == "review_close":
            self.model.review_held = None
            return
        directory = safe_path(self.state, "learning")
        pointer = read_json(directory / "pending-review.json")
        if not pointer.get("review_file"):
            self.model.review_held = None
            self.emit("notice", key="review_missing")
            return
        path = safe_path(directory, pointer["review_file"])
        draft = checked_json(path)
        evaluation_path = safe_path(directory, draft["evaluation_file"])
        checksum = digest_file(evaluation_path, cancel)
        if checksum != draft.get("evaluation_sha256"):
            raise YuanError(tr("review_stale", self.language))
        evaluation = checked_json(evaluation_path)
        if command == "review_open":
            self.model.review_held = pointer.get("model_sha256")
            self.emit("review_snapshot", evaluation=evaluation, review=draft, evaluation_sha256=checksum)
            return
        if action.get("evaluation_sha256") != checksum:
            raise YuanError(tr("review_stale", self.language))
        if command == "review_play":
            field = action.get("field")
            case = next((item for item in evaluation["cases"] if item["id"] == action.get("case")), None)
            if field not in ("input", "output") or case is None:
                raise YuanError(tr("review_stale", self.language))
            audio_path = safe_path(directory, case[field + "_file"])
            if digest_file(audio_path, cancel) != case[field + "_sha256"]:
                raise InvalidAudio("听测音频摘要不匹配 / Listening audio digest mismatch")
            import soundfile as sf
            values, rate = sf.read(str(audio_path), dtype="float32", always_2d=False)
            if values.ndim != 1 or not values.size or not self.model.np.isfinite(values).all():
                raise InvalidAudio("听测音频无效 / Invalid listening audio")
            gain = self.model.policy.number("acceptance", "preview_gain", 1 / math.sqrt(10), 0.0, 1.0)
            try:
                self.devices.open_streams(cancel)
                self.devices.play(values * gain, rate, cancel)
            finally:
                self.devices.close_streams()
                self.emit("status", key="ready" if self.model.playback_ready else "preparing_model")
            return
        if command == "review_submit":
            checks = action.get("checks")
            reviewer = action.get("reviewer")
            required = ("understandable", "follows_prompt", "relevant_reply", "ends_normally", "no_abnormal_audio")
            if not isinstance(reviewer, str) or not reviewer.strip() or not isinstance(checks, list) or len(checks) != len(evaluation["cases"]):
                raise YuanError(tr("review_incomplete", self.language))
            if any(not isinstance(item, dict) or not isinstance(item.get("id"), str) or any(type(item.get(key)) is not bool for key in required) for item in checks):
                raise YuanError(tr("review_incomplete", self.language))
            if {item["id"] for item in checks} != {item["id"] for item in evaluation["cases"]}:
                raise YuanError(tr("review_stale", self.language))
            draft.update(reviewer=reviewer.strip(), reviewed_at=time.time(), checks=checks)
            atomic_json(path, draft, cancel)
            self.model.review_held = None
            self.model.refresh_release()
            self.emit("notice", key="review_saved")

    def import_pair(self, action, cancel):
        import soundfile as sf
        required = ("recording_id", "reviewer", "prompt", "license", "user_speaker", "assistant_speaker", "user_file", "assistant_file")
        if action.get("confirmed") is not True or action.get("language") not in ("中文", "English") or not all(isinstance(action.get(key), str) and action[key].strip() for key in required):
            raise YuanError(tr("required_fields", self.language))
        if action["user_speaker"].strip() == action["assistant_speaker"].strip():
            raise YuanError(tr("required_fields", self.language))
        split = action.get("split", "auto")
        if split not in ("auto", "train", "validation", "guard", "release"):
            raise YuanError(tr("required_fields", self.language))
        endings = {role: action.get(role + "_utterance_final") for role in ("user", "assistant")}
        if any(value is not None and not isinstance(value, bool) for value in endings.values()):
            raise YuanError("语句结束标注必须为完整、截断或未知 / Utterance ending must be complete, truncated or unknown")
        recording = action["recording_id"].strip()
        existing = self.store.db.execute("SELECT split FROM groups WHERE source_key=? LIMIT 1", (recording,)).fetchone()
        if split == "auto":
            if existing:
                split = existing[0]
            else:
                missing = self.learning_state()["missing"]
                split = next((row["split"] for row in missing if row["language"] == action["language"]), "train")
        annotation = uuid.uuid4().hex
        root = safe_path(self.state, "datasets")
        root.mkdir(parents=True, exist_ok=True)
        assets = safe_path(root, "assets/" + annotation)
        assets.mkdir(parents=True, exist_ok=False)
        turns = []
        published = False
        try:
            offset = 0.0
            for role, field in (("user", "user_file"), ("assistant", "assistant_file")):
                check(cancel)
                source = Path(action[field]).expanduser().resolve(strict=True)
                before = source.stat()
                if not source.is_file() or before.st_size <= 0 or before.st_size > shutil.disk_usage(root).free // 2:
                    raise OSError("音频文件或可用磁盘空间无效 / Invalid audio file or insufficient disk space")
                target = assets / (role + source.suffix.lower())
                block = max(1, min(1024 * 1024, math.isqrt(before.st_size)))
                with source.open("rb") as incoming, target.open("xb") as outgoing:
                    while True:
                        check(cancel)
                        part = incoming.read(block)
                        if not part:
                            break
                        outgoing.write(part)
                    outgoing.flush()
                    os.fsync(outgoing.fileno())
                after = source.stat()
                if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    raise OSError("复制期间源音频发生变化 / Source audio changed while copying")
                info = sf.info(str(target))
                if info.frames <= 0 or info.samplerate <= 0:
                    raise InvalidAudio("录音为空 / Recording is empty")
                turn = {"id": annotation + "-" + role, "role": role, "speaker": action[role + "_speaker"].strip(),
                        "audio": target.relative_to(root).as_posix(), "sha256": digest_file(target, cancel), "start_seconds": offset, "utterance_final": endings[role]}
                if role == "assistant":
                    turn.update(reply_to=turns[0]["id"], response_verified=True)
                turns.append(turn)
                offset += info.frames / info.samplerate
            data = {"format": "yuan-paired-audio-v1", "origin": "human_recording", "recording_id": recording, "language": action["language"],
                    "reviewer": action["reviewer"].strip(), "annotation_id": annotation, "prompt": action["prompt"].strip(),
                    "license": action["license"].strip(), "split": split, "turns": turns}
            atomic_json(root / (annotation + ".json"), data, cancel)
            published = True
            self.emit("notice", key="pair_submitted", recording_id=recording, split=split, annotation=annotation)
        finally:
            if not published:
                shutil.rmtree(assets, ignore_errors=True)

@contextmanager
def runtime_entry(state):
    state = Path(state).resolve()
    state.mkdir(parents=True, exist_ok=True)
    env = workspace_environment(state.parent)
    os.environ.clear()
    os.environ.update(env)
    tempfile.tempdir = env["TMPDIR"]
    os.chdir(state)
    output = os.fdopen(os.dup(sys.stdout.fileno()), "w", encoding="utf-8", buffering=1)
    events = EventStream(output)
    emit = events.emit
    read_fd, write_fd = os.pipe()
    os.dup2(write_fd, sys.stdout.fileno())
    os.dup2(write_fd, sys.stderr.fileno())
    os.close(write_fd)
    def capture():
        with os.fdopen(read_fd, "r", encoding="utf-8", errors="replace") as stream:
            for line in stream:
                value = line.rstrip("\r\n")
                if events.cancel.is_set():
                    return
    recorder = threading.Thread(target=capture, daemon=True, name="YuanRuntimeCapture")
    recorder.start()
    import logging
    logging.basicConfig(level=logging.WARNING)
    try:
        yield emit, events.cancel
    finally:
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
                os.dup2(output.fileno(), stream.fileno())
            except (OSError, ValueError):
                pass
        recorder.join(timeout=max(1, math.sqrt(os.cpu_count() or 1)))
        if events.close():
            output.close()

def engine_entry(state):
    state = Path(state).resolve()
    with runtime_entry(state) as (emit, cancel):
        try:
            with WorkspaceLease(state / "engine.lock"):
                Engine(state, emit, shutdown=cancel).run()
        except Exception as exc:
            emit("error", key="failure", reason=concise_error(exc), fatal=True, fault=fault_event(exc))

class LiveStatus:
    def __init__(self, state):
        self.lock = threading.RLock()
        self.closed = False
        self.sequence = 0
        self.latest = {}

    def add(self, event):
        kind = event.get("type")
        if kind not in ("status", "error", "notice", "deployment", "learning", "network", "storage_state", "recovery"):
            return
        with self.lock:
            if self.closed:
                return
            self.sequence += 1
            value = {key: event[key] for key in ("at", "type", "key", "reason", "action_key") if key in event}
            self.latest[kind] = (self.sequence, value)

    def page(self, before=None, limit=None):
        with self.lock:
            rows = sorted((row for row in self.latest.values() if before is None or row[0] < before), key=lambda row: row[0])
            return rows[-max(1, int(limit or len(rows) or 1)):] if not self.closed else []

    def close(self):
        with self.lock:
            self.latest.clear()
            self.closed = True

def event_text(event, language, compact=False):
    moment = time.strftime("%H:%M:%S", time.localtime(event.get("at", time.time())))
    kind = event.get("key") or event.get("type", "")
    title = tr(kind, language)
    values = {key: value for key, value in redact(event).items() if key not in ("at", "type", "key", "generation", "_sequence")}
    if compact:
        fields = []
        if event.get("type") == "operation":
            fields.append(tr("operation_" + str(event.get("outcome", "")), language))
        fault = event.get("fault")
        if isinstance(fault, dict):
            fields.append(tr("fault_" + fault.get("category", "unknown"), language))
            if fault.get("stage"):
                fields.append(tr(fault["stage"], language))
        for key in ("component", "version", "file", "check"):
            if values.get(key):
                fields.append(str(values[key]))
        if isinstance(event.get("elapsed"), (int, float)):
            fields.append(f"{event['elapsed']:.2f}s")
        if event.get("type") == "attempt":
            fields.append(str(event.get("attempt_number", "")))
        if event.get("action_key"):
            fields.append(tr(event["action_key"], language))
        detail = values.get("reason") or (fault.get("reason") if isinstance(fault, dict) else None) or values.get("detail") or ""
        if detail:
            summary = local_message(detail, language).split("\n", 1)[0][:240]
            if summary not in fields:
                fields.append(summary)
        if isinstance(fault, dict) and fault.get("code"):
            fields.append(str(fault["code"]))
        body = " · ".join(value for value in fields if value)
    else:
        body = json.dumps(values, ensure_ascii=False, allow_nan=False, indent=2) if values else ""
    return moment + "  " + title + ((" · " if compact else "\n") + body if body else "") + "\n"

class Mailbox:
    def __init__(self):
        self.events = deque()
        self.latest = {}
        self.lock = threading.Lock()
        self.sequence = 0

    def put(self, event):
        with self.lock:
            self.sequence += 1
            event = {**event, "_sequence": self.sequence}
            if event.get("type") in ("progress", "source_progress", "readiness", "phase", "audio", "metrics", "devices", "model", "supervision", "condition_check", "startup_resources"):
                key = (event.get("generation"), event.get("attempt_id"), event.get("phase_id"), event["type"], event.get("id"))
                if event["type"] in ("audio", "metrics", "devices", "model"):
                    event = {**self.latest.get(key, {}), **event}
                self.latest[key] = event
            else:
                self.events.append(event)
            return self.sequence

    def drain(self):
        with self.lock:
            events = list(self.events) + list(self.latest.values())
            self.events.clear()
            self.latest.clear()
        return sorted(events, key=lambda event: event["_sequence"])

class EventStream:
    def __init__(self, stream):
        self.stream = stream
        self.mailbox = Mailbox()
        self.wake = threading.Event()
        self.closing = threading.Event()
        self.cancel = threading.Event()
        self.failed = threading.Event()
        self.lock = threading.Lock()
        self.sent = threading.Condition()
        self.sent_sequence = 0
        self.phase_id = None
        self.attempt_id = os.environ.get("YUAN_ATTEMPT_ID")
        self.flush_seconds = max(1.0, math.sqrt(os.cpu_count() or 1))
        self.thread = threading.Thread(target=self.run, daemon=True, name="YuanEvents")
        self.thread.start()

    def emit(self, kind, **values):
        values = redact(values)
        with self.lock:
            if self.closing.is_set() or self.failed.is_set():
                return
            if kind == "status":
                self.phase_id = values.setdefault("phase_id", uuid.uuid4().hex)
            if kind in ("phase", "progress"):
                values.setdefault("phase_id", self.phase_id)
            sequence = self.mailbox.put({"type": kind, "at": time.time(), "attempt_id": self.attempt_id, **values})
            self.wake.set()
        if kind == "operation" and values.get("outcome") == "begin":
            deadline = time.monotonic() + self.flush_seconds
            with self.sent:
                while self.sent_sequence < sequence and not self.failed.is_set():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self.failed.set()
                        self.cancel.set()
                        raise EngineFault("阶段记录通道超时 / Stage event channel timed out")
                    self.sent.wait(remaining)
            if self.failed.is_set():
                raise EngineFault("阶段记录通道已断开 / Stage event channel disconnected")

    def run(self):
        try:
            while True:
                self.wake.wait()
                self.wake.clear()
                events = self.mailbox.drain()
                if events:
                    payload = "".join(json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n" for event in events)
                    self.stream.write(payload)
                    self.stream.flush()
                    with self.sent:
                        self.sent_sequence = max(self.sent_sequence, max(event["_sequence"] for event in events))
                        self.sent.notify_all()
                if self.closing.is_set():
                    if self.mailbox.sequence <= self.sent_sequence:
                        break
                    self.wake.set()
        except (OSError, ValueError, TypeError):
            self.failed.set()
            self.cancel.set()
        finally:
            with self.sent:
                self.sent.notify_all()

    def close(self):
        with self.lock:
            self.closing.set()
            self.wake.set()
        self.thread.join(timeout=self.flush_seconds)
        if self.thread.is_alive():
            self.failed.set()
            self.cancel.set()
        return not self.thread.is_alive()

class ProcessOutput:
    def __init__(self, process):
        self.process = process
        self.lines = queue.Queue(maxsize=max(32, (os.cpu_count() or 1) * 4))
        self.stopped = threading.Event()
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True, name="YuanEngineOutput")
        self.thread.start()

    def run(self):
        try:
            while not self.stopped.is_set():
                line = self.process.stdout.readline()
                if not line:
                    return
                while not self.stopped.is_set():
                    try:
                        self.lines.put(line, timeout=math.sqrt(tick()))
                        break
                    except queue.Full:
                        pass
        except (OSError, ValueError):
            pass
        finally:
            self.done.set()
            self.process.stdout.close()

    def close(self, timeout):
        self.stopped.set()
        self.thread.join(timeout=max(0.0, timeout))
        return not self.thread.is_alive()

class RecoveryGate:
    def __init__(self, state, emit, clock=None):
        self.state, self.emit, self.clock = Path(state), emit, clock or time.monotonic
        self.path = safe_path(self.state, "recovery.json")
        self.policy = AdaptivePolicy(self.state)
        initial = self.policy.number("supervision", "initial_phase_seconds", 120.0, tick(), 86400.0)
        self.total = self.policy.number("recovery", "startup_campaign_seconds", initial * 16, tick(), 86400.0)
        self.transfer_total = self.policy.number("recovery", "startup_transfer_seconds", self.total * 4, tick(), 86400.0)
        self.per_fault = self.policy.integer("recovery", "same_fault_attempts", 3, 1, 16)
        self.maximum = self.policy.integer("recovery", "total_attempts", 6, 1, 64)
        self.delay = self.policy.number("recovery", "retry_delay_seconds", math.sqrt(initial), tick(), self.total)
        self.delay_ceiling = self.policy.number("recovery", "retry_max_seconds", initial, self.delay, self.total)
        self.check_interval = self.policy.number("recovery", "condition_check_seconds", max(5.0, math.sqrt(initial)), tick(), 3600.0)
        self.stable_seconds = self.policy.number("recovery", "stable_runtime_seconds", initial, tick(), 86400.0)
        self.code = digest_file(Path(__file__))
        saved = read_json(self.path)
        valid = saved.get("code") == self.code and saved.get("conditions") == self.conditions()
        self.records = saved.get("faults", {}) if valid else {}
        if not isinstance(self.records, dict):
            self.records = {}
        used = saved.get("used_seconds", 0.0) if valid else 0.0
        self.used = float(used) if isinstance(used, (float, int)) and not isinstance(used, bool) and math.isfinite(used) and used >= 0 else 0.0
        transferred = saved.get("transfer_used_seconds", 0.0) if valid else 0.0
        self.transfer_used = float(transferred) if isinstance(transferred, (int, float)) and not isinstance(transferred, bool) and math.isfinite(transferred) and transferred >= 0 else 0.0
        attempts = saved.get("attempts", 0) if valid else 0
        self.attempts = int(attempts) if isinstance(attempts, int) and not isinstance(attempts, bool) and attempts >= 0 else 0
        self.last = saved.get("last", {}) if valid else {}
        if not isinstance(self.last, dict):
            self.last = {}
        self.policy.save()

    @property
    def remaining(self):
        return max(0.0, self.total - self.used)

    @property
    def transfer_remaining(self):
        return max(0.0, self.transfer_total - self.transfer_used)

    def conditions(self):
        paths = [Path(__file__).resolve(), self.state / "policy.json", self.state / "network.json", self.state / "runtime-current.json", self.state / "runtime-lock.json"]
        paths.append(self.state / "runtime-build.json")
        for root in (self.state.parent, Path(__file__).resolve().parent):
            paths.extend(root / name for name in ("Yuan.release.json", "Yuan.trust.json", "Yuan.model.json", "Yuan.acceptance.json"))
            source = read_json(root / "Yuan.release.json").get("source")
            if isinstance(source, str) and not urllib.parse.urlparse(source).scheme:
                try:
                    paths.append(safe_path(root, source))
                except YuanError:
                    pass
        for name in ("owned-release.json", "owned-release-pending.json"):
            paths.append(self.state / name)
            owned = read_json(self.state / name)
            if owned.get("directory"):
                try:
                    paths.append(safe_path(self.state, owned["directory"]) / "release-manifest.json")
                except (TypeError, YuanError):
                    pass
        for name in ("runtime-current.json", "runtime-build.json"):
            pointer = read_json(self.state / name)
            if pointer.get("directory"):
                try:
                    runtime = safe_path(self.state, pointer["directory"])
                    paths += [runtime / "ready.json", runtime / "lock.json", runtime / "pyvenv.cfg", runtime_python(runtime)]
                except YuanError:
                    pass
        try:
            config = NetworkConfig(self.state)
            configuration = [config.index_urls, config.proxy, config.ca_file, config.client_cert]
            paths += [Path(value) for value in (config.ca_file, config.client_cert) if value]
        except Exception as exc:
            configuration = [failure_category(exc), concise_error(exc)]
        stamps = []
        for path in paths:
            try:
                info = path.stat()
                stamps.append([str(path), info.st_size, info.st_mtime_ns])
            except OSError:
                stamps.append([str(path), None, None])
        return payload_digest([stamps, configuration])

    def save(self):
        try:
            atomic_json(self.path, {"code": self.code, "conditions": self.conditions(), "faults": self.records, "last": self.last,
                                    "used_seconds": self.used, "transfer_used_seconds": self.transfer_used, "attempts": self.attempts, "updated": time.time()})
        except (OSError, YuanError) as exc:
            self.emit("notice", key="journal_failure", component="recovery", reason=concise_error(exc))

    def record(self, fault, seconds, identity=None, transfer_seconds=0.0):
        details = fault.get("details", {})
        fingerprint = payload_digest([identity or runtime_identity(self.state), fault.get("category"), fault.get("code"), fault.get("stage"),
                                      details.get("component"), details.get("package")])
        row = self.records.setdefault(fingerprint, {"count": 0})
        if not isinstance(row, dict):
            row = self.records[fingerprint] = {"count": 0}
        previous = row.get("count", 0)
        row["count"] = (previous if isinstance(previous, int) and not isinstance(previous, bool) and previous >= 0 else 0) + 1
        self.used += max(0.0, seconds)
        self.transfer_used += max(0.0, transfer_seconds)
        self.attempts += 1
        self.last = {**redact(fault), "fingerprint": fingerprint, "count": row["count"], "at": time.time()}
        row.update(code=fault.get("code"), stage=fault.get("stage"), category=fault.get("category"), at=time.time())
        self.save()
        return self.last

    def retry_blocker(self):
        if self.remaining <= tick():
            return "retry_time_exhausted"
        if self.transfer_remaining <= tick():
            return "retry_transfer_exhausted"
        if self.attempts >= self.maximum:
            return "retry_total_exhausted"
        if self.last.get("count", 0) >= self.per_fault:
            return "retry_same_fault_exhausted"
        if self.last.get("details", {}).get("retry_blocker") == "retry_import_unchanged":
            return "retry_import_unchanged"
        return None

    def can_retry(self):
        return self.retry_blocker() is None

    def reset(self, reason):
        self.records, self.last, self.used, self.transfer_used, self.attempts = {}, {}, 0.0, 0.0, 0
        fresh = RecoveryGate(self.state, self.emit, self.clock)
        for field in ("policy", "total", "transfer_total", "per_fault", "maximum", "delay", "delay_ceiling", "check_interval", "stable_seconds", "code"):
            setattr(self, field, getattr(fresh, field))
        self.save()
        self.emit("notice", key=reason)

    def retry_delay(self):
        return min(self.delay_ceiling, self.delay * 2 ** min(max(0, self.last.get("count", 1) - 1), self.maximum), self.remaining)

class Launcher:
    def __init__(self, workspace, generation, mailbox, lease=None):
        self.workspace, self.generation, self.mailbox = Path(workspace).resolve(), generation, mailbox
        self.cancel = threading.Event()
        self.retry_requested = threading.Event()
        self.blocked = threading.Event()
        self.process_retained = False
        self.process = None
        self.lock = threading.RLock()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.lease = lease
        self.send_lock = threading.Lock()
        self.commands = queue.Queue()
        self.sender = threading.Thread(target=self.send_loop, daemon=True, name="YuanCommands")
        self.activity = LiveStatus(safe_path(self.workspace, STATE_NAME))
        self.phase_at = time.monotonic()
        self.phase_key = "setup"
        self.retry_until = None
        self.activity_error = None
        self.event_lock = threading.RLock()
        self.event_sequence = 0
        self.attempt_id = None
        self.attempt_number = 0
        self.phase_id = None
        self.supervisor = StageSupervisor(safe_path(self.workspace, STATE_NAME), self.emit)
        self.control_deadline = None
        self.control_id = None
        self.child_error = None
        self.child_fault = None
        self.engine_ever_ready = False
        self.engine_ready_at = None
        self.gate = RecoveryGate(safe_path(self.workspace, STATE_NAME), self.emit)
        self.sender.start()
        self.thread.start()

    def emit(self, kind, **values):
        values = redact(values)
        with self.event_lock:
            self.event_sequence += 1
            if kind == "status":
                self.phase_at = time.monotonic()
                self.phase_key = values.get("key", self.phase_key)
                self.phase_id = values.setdefault("phase_id", uuid.uuid4().hex)
                self.retry_until = None
            elif kind == "recovery":
                self.retry_until = time.monotonic() + max(0, float(values["seconds"])) if values.get("mode") == "automatic_retry" and values.get("seconds") is not None else None
            if kind in ("progress", "phase"):
                values.setdefault("phase_id", self.phase_id)
            event = {"at": time.time(), **values, "type": kind, "generation": self.generation, "attempt_id": self.attempt_id,
                     "attempt_number": self.attempt_number, "event_sequence": self.event_sequence}
            try:
                self.activity.add(event)
                if self.activity_error:
                    recovered = {"type": "notice", "generation": self.generation, "at": time.time(), "key": "recording_resumed",
                                 "attempt_id": self.attempt_id, "attempt_number": self.attempt_number}
                    self.activity.add(recovered)
                    self.mailbox.put(recovered)
                    self.activity_error = None
            except (sqlite3.Error, OSError, ValueError) as exc:
                reason = concise_error(exc)
                if reason != self.activity_error:
                    self.mailbox.put({"type": "notice", "generation": self.generation, "at": time.time(), "key": "journal_failure", "reason": reason,
                                      "attempt_id": self.attempt_id, "attempt_number": self.attempt_number})
                self.activity_error = reason
            self.mailbox.put(event)

    def send(self, action):
        with self.lock:
            process = self.process
            if not process or process.poll() is not None:
                return False
            if action.get("action") in ("stop", "close"):
                self.control_deadline = time.monotonic() + self.supervisor.grace
                self.control_id = action.get("id")
            self.commands.put((process, dict(action)))
        return True

    def send_loop(self):
        while True:
            item = self.commands.get()
            if item is None:
                return
            process, action = item
            with self.lock:
                current = self.process
            if process is not current or process.poll() is not None:
                continue
            try:
                with self.send_lock:
                    process.stdin.write(json.dumps(action, ensure_ascii=False, allow_nan=False) + "\n")
                    process.stdin.flush()
            except (OSError, ValueError, TypeError) as exc:
                self.emit("notice", key="command_failed", action=action.get("action"), id=action.get("id"), pid=process.pid, reason=concise_error(exc))
                with self.lock:
                    if self.process is process:
                        self.control_deadline = time.monotonic()

    def request_recheck(self):
        if self.cancel.is_set() or not self.blocked.is_set():
            return False
        self.retry_requested.set()
        return True

    def wait_recovery(self, fault, installer=None):
        self.blocked.set()
        category = fault.get("category", "unknown")
        target = fault.get("details", {}).get("url")
        if not isinstance(target, str) or urllib.parse.urlsplit(target).scheme != "https" or "<redacted>" in target:
            target = installer.network_config.index_urls[0] if installer is not None else None
        initial_conditions = self.gate.conditions()
        try:
            initial_free = shutil.disk_usage(self.workspace).free
        except OSError:
            initial_free = None
        def writable():
            try:
                state = safe_path(self.workspace, STATE_NAME)
                fd, path = tempfile.mkstemp(dir=state if state.is_dir() else self.workspace, prefix=".yuan-write-check-")
                os.close(fd)
                os.unlink(path)
                return True
            except (OSError, YuanError):
                return False
        initially_writable = writable() if category == "storage" else True
        interrupted = AnyCancel(self.cancel, self.retry_requested)
        def publish(mode, seconds=None):
            action = "action_" + category if "action_" + category in TEXT else "action_unknown"
            key = "retry" if mode == "automatic_retry" else "network_wait" if category == "network" and mode == "waiting_conditions" else "startup_blocked"
            self.emit("status", key=key)
            self.emit("recovery", key="backoff" if mode == "automatic_retry" else key, mode=mode, blocked=True, task_running=False,
                      seconds=seconds if mode == "automatic_retry" else None, fault=redact(fault), action_key=action,
                      exhausted=not self.gate.can_retry(), retry_blocker=self.gate.retry_blocker() or ("retry_requires_change" if mode != "automatic_retry" else None),
                      matching_failures=self.gate.last.get("count", 0), same_fault_limit=self.gate.per_fault,
                      attempts=self.gate.attempts, attempts_limit=self.gate.maximum, remaining_seconds=self.gate.remaining, transfer_remaining_seconds=self.gate.transfer_remaining)
        if category in ("timeout", "engine", "unknown") and self.gate.can_retry():
            delay = self.gate.retry_delay()
            publish("automatic_retry", delay)
            began = time.monotonic()
            interrupted.wait(delay)
            self.gate.used += max(0.0, time.monotonic() - began)
            self.gate.save()
            if not interrupted.is_set() and self.gate.can_retry():
                return True
        mode = "paused" if not self.gate.can_retry() else "waiting_conditions"
        publish(mode)
        while not self.cancel.is_set():
            if self.retry_requested.is_set():
                self.retry_requested.clear()
                self.gate.reset("recheck_requested")
                return True
            if self.gate.conditions() != initial_conditions:
                self.gate.reset("conditions_changed")
                return True
            if category == "storage" and initial_free is not None:
                try:
                    free = shutil.disk_usage(self.workspace).free
                    margin = self.gate.policy.integer("recovery", "storage_growth_bytes", 1024 * 1024, 4096, max(4096, shutil.disk_usage(self.workspace).total))
                    if (free - initial_free >= margin or not initially_writable) and writable():
                        self.gate.reset("conditions_changed")
                        return True
                except OSError:
                    pass
            if category == "network" and self.gate.can_retry() and target:
                began = time.monotonic()
                reachable = False
                try:
                    guard = StartupControl(interrupted, began + self.gate.remaining)
                    network = Network(guard, self.gate.policy)
                    try:
                        response = network.request(target, method="HEAD", deadline=guard.deadline)
                    except urllib.error.HTTPError as exc:
                        if exc.code not in (405, 501):
                            raise
                        exc.close()
                        response = network.request(target, deadline=guard.deadline)
                    response.close()
                    reachable = True
                except Cancelled:
                    continue
                except Exception as exc:
                    checked = fault_event(exc)
                    self.emit("condition_check", key="network_wait", fault=checked, source=target)
                    if checked["category"] in ("integrity", "configuration", "resolution"):
                        category = checked["category"]
                        fault = {**checked, "stage": fault.get("stage")}
                        mode = "waiting_conditions"
                        publish(mode)
                finally:
                    self.gate.used += max(0.0, time.monotonic() - began)
                    self.gate.save()
                if reachable and self.gate.can_retry():
                    self.emit("notice", key="network_restored", source=target)
                    return True
                if not self.gate.can_retry() and mode != "paused":
                    mode = "paused"
                    publish(mode)
            self.emit("condition_check", key="condition_check", blocked=True, mode=mode, task_running=False,
                      changed=False, next_check_seconds=self.gate.check_interval)
            interrupted.wait(self.gate.check_interval)
        return False

    def stop(self):
        if self.cancel.is_set():
            return
        self.cancel.set()
        self.send({"action": "close"})

    def monitor_process(self, process):
        output = ProcessOutput(process)
        self.supervisor.start(process.pid, remaining=self.startup_guard.deadline - time.monotonic())
        drain_deadline = None
        try:
            while True:
                now = time.monotonic()
                with self.lock:
                    if self.cancel.is_set() and self.control_deadline is None:
                        self.control_deadline = now + self.supervisor.grace
                    control_deadline = self.control_deadline
                if control_deadline is not None and now >= control_deadline:
                    if self.cancel.is_set():
                        raise Cancelled()
                    raise EngineFault("控制命令未按时完成，存档状态未确认 / Control command timed out; storage state unconfirmed: PID " + str(process.pid))
                if not self.cancel.is_set() and control_deadline is None:
                    self.supervisor.poll()
                try:
                    line = output.lines.get(timeout=math.sqrt(tick()))
                except queue.Empty:
                    line = None
                if line is not None:
                    try:
                        event = json.loads(line)
                    except (ValueError, TypeError):
                        self.emit("runtime_message", detail=line.rstrip("\r\n"), pid=process.pid)
                        event = None
                    if isinstance(event, dict) and isinstance(event.get("type"), str):
                        incoming_attempt = event.get("attempt_id")
                        if incoming_attempt is not None and incoming_attempt != self.attempt_id:
                            continue
                        self.supervisor.observe(event)
                        kind = event.pop("type")
                        if kind == "ready":
                            self.engine_ever_ready = True
                            self.engine_ready_at = time.monotonic()
                        if kind == "error":
                            self.child_error = event.get("reason")
                            if isinstance(event.get("fault"), dict):
                                self.child_fault = event["fault"]
                        if kind == "exit" or kind == "error" and event.get("fatal"):
                            with self.lock:
                                limit = time.monotonic() + self.supervisor.grace
                                self.control_deadline = min(self.control_deadline, limit) if self.control_deadline is not None else limit
                        if kind == "ended":
                            with self.lock:
                                if not self.cancel.is_set() and event.get("id") == self.control_id:
                                    self.control_deadline = None
                                    self.control_id = None
                        event.pop("generation", None)
                        event.pop("event_sequence", None)
                        self.emit(kind, **event)
                if process.poll() is not None:
                    if output.done.is_set() and output.lines.empty():
                        break
                    if drain_deadline is None:
                        drain_deadline = time.monotonic() + self.supervisor.grace
                    if time.monotonic() >= drain_deadline:
                        raise EngineFault("进程已退出但输出通道未关闭 / Process exited but output channel did not close: PID " + str(process.pid))
                elif output.done.is_set() and output.lines.empty():
                    if drain_deadline is None:
                        drain_deadline = time.monotonic() + self.supervisor.grace
                    if time.monotonic() >= drain_deadline:
                        raise EngineFault("引擎输出通道已关闭但进程仍在运行 / Engine output closed while process remains running: PID " + str(process.pid))
            return process.returncode
        finally:
            output.close(math.sqrt(tick()))

    def run(self):
        state = safe_path(self.workspace, STATE_NAME)
        retained_process = False
        installer = None
        try:
            if self.lease is None:
                self.lease = WorkspaceLease(state / "workspace.lock")
            with WorkspaceLease(state / "engine.lock"):
                pass
            if self.gate.last and not self.gate.can_retry():
                self.emit("error", key="failure", reason=self.gate.last.get("reason", ""), fault=self.gate.last)
                if not self.wait_recovery(self.gate.last):
                    return
            while not self.cancel.is_set():
                self.blocked.clear()
                self.attempt_started = time.monotonic()
                with self.event_lock:
                    self.attempt_number += 1
                    self.attempt_id = uuid.uuid4().hex
                    self.phase_id = None
                    self.emit("attempt", key="attempt")
                with self.lock:
                    self.control_deadline = self.control_id = None
                self.child_error = self.child_fault = None
                self.engine_ever_ready = False
                self.engine_ready_at = None
                failure = None
                installer = None
                self.startup_guard = None
                try:
                    guard = StartupControl(self.cancel, self.attempt_started + self.gate.remaining, transfer_seconds=self.gate.transfer_remaining)
                    self.startup_guard = guard
                    check(guard)
                    self.supervisor = StageSupervisor(state, self.emit)
                    installer = RuntimeInstaller(self.workspace, guard, self.emit)
                    installer.env["YUAN_ATTEMPT_ID"] = self.attempt_id
                    owned_release_preflight(state, self.emit)
                    interpreter = installer.ensure()
                    check(guard)
                    self.supervisor = StageSupervisor(state, self.emit, identity=runtime_identity(state, installer.verified_entry))
                    self.emit("status", key="loading")
                    budgets = {"stage_import_" + name: self.supervisor.budget("stage_import_" + name) for name in RUNTIME_MODULES}
                    self.supervisor.policy.save()
                    env = {**installer.env, "YUAN_ATTEMPT_ID": self.attempt_id, "YUAN_IMPORT_BUDGETS": json.dumps(budgets),
                           "YUAN_IMPORT_IDENTITY": self.supervisor.fingerprint, "YUAN_STARTUP_DEADLINE": str(guard.deadline)}
                    process = spawn([str(interpreter), "-B", str(Path(__file__).resolve()), "--engine", str(state)], env,
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1)
                    with self.lock:
                        self.process = process
                    returncode = self.monitor_process(process)
                    check(self.cancel)
                    self.emit("disconnected", returncode=returncode, pid=process.pid, reason=self.child_error)
                    if self.child_fault:
                        details = dict(self.child_fault.get("details", {}))
                        raise YuanFault(self.child_fault.get("reason") or self.child_error or "引擎失败 / Engine failed", category=self.child_fault.get("category", "engine"),
                                        code=self.child_fault.get("code", "engine_failure"), stage=self.child_fault.get("stage"), **details)
                    raise EngineFault(self.child_error or "音频引擎意外退出 / Audio engine exited unexpectedly", code="engine_exited", stage=self.phase_key, returncode=returncode, pid=process.pid)
                except Cancelled:
                    break
                except Exception as exc:
                    failure = fault_details(exc, installer.phase if installer is not None and self.process is None else self.phase_key)
                    if failure["code"] == "process_reap_failed" or failure.get("details", {}).get("process_retained"):
                        retained_process = True
                        self.cancel.set()
                    self.emit("error", key="failure", reason=concise_error(exc), fault=redact(failure), component="launcher")
                finally:
                    self.supervisor.flush_history(force=True)
                    with self.lock:
                        process = self.process
                    if process:
                        try:
                            if process.poll() is None:
                                end_process(process)
                            self.emit("process_reaped", pid=process.pid, returncode=process.returncode)
                            with self.lock:
                                self.process = None
                            if process.stdin:
                                def close_input(retired=process):
                                    try:
                                        with self.send_lock:
                                            retired.stdin.close()
                                    except (OSError, ValueError):
                                        pass
                                closer = threading.Thread(target=close_input, daemon=True, name="YuanPipeCleanup")
                                closer.start()
                                closer.join(timeout=math.sqrt(tick()))
                        except Exception as exc:
                            retained_process = True
                            self.cancel.set()
                            self.emit("error", key="failure", reason=concise_error(exc), fault=fault_event(exc), pid=process.pid)
                if failure is not None and not self.cancel.is_set():
                    now = time.monotonic()
                    stable = self.engine_ready_at is not None and now - self.engine_ready_at >= self.gate.stable_seconds
                    if stable:
                        self.gate.reset("conditions_changed")
                    transfer_used = self.startup_guard.transfer_used if self.startup_guard is not None and not stable else 0.0
                    elapsed = 0.0 if stable else max(0.0, now - self.attempt_started - transfer_used)
                    self.gate.record(failure, elapsed, identity=installer.supervisor.fingerprint if installer is not None else None, transfer_seconds=transfer_used)
                    if not self.wait_recovery(failure, installer):
                        break
        except Cancelled:
            pass
        except Exception as exc:
            self.emit("error", key="failure", reason=concise_error(exc), fault=fault_event(exc))
        finally:
            self.blocked.clear()
            self.commands.put(None)
            self.sender.join(timeout=max(tick(), math.sqrt(tick())))
            self.process_retained = retained_process
            if self.lease and not retained_process:
                self.lease.close()
            self.emit("launcher_closed", process_retained=retained_process)

class YuanApp:
    def __init__(self, root):
        import tkinter as tk
        from tkinter import ttk, font
        self.tk, self.ttk, self.root = tk, ttk, root
        self.mailbox = Mailbox()
        self.launcher = None
        self.workspace = None
        self.generation = 0
        self.attempt_id = None
        self.attempt_number = 0
        self.event_sequence = 0
        self.phase_id = None
        self.supervision_value = {}
        self.current_failure = {}
        self.last_failure = {}
        self.recovery_value = {}
        self.startup_values = {}
        self.deployment_value = {}
        self.notice_context = None
        self.ready = False
        self.runtime_ready = False
        self.readiness_values = {}
        self.session_id = None
        self.session_started = None
        self.ending = False
        self.closing = False
        self.language = "中文"
        self.status_key = "idle"
        self.learning_key = "waiting_data"
        self.network_key = "wait"
        self.reason = ""
        self.notice_key = ""
        self.device_values = {}
        self.model_values = {}
        self.audio_values = {}
        self.learning_values = {}
        self.resource_values = {}
        self.progress_value = {}
        self.source_values = {}
        self.phase_values = {}
        self.activity_window = None
        self.learning_window = self.review_window = None
        self.recent_activity = deque()
        self.pipeline_verified = False
        self.level = 0.0
        self.phase = 0.0
        self.last_draw = time.monotonic()
        self.last_saved_prompt = None
        self.save_job = None
        self.pending_workspace = None
        self.palette = {"bg": "#090d16", "panel": "#121a28", "field": "#0c1320", "border": "#25334a", "text": "#f0f4ff", "muted": "#a5b4cd",
                        "accent": "#79e7d2", "lavender": "#baacff", "warning": "#ffc891"}
        families = set(font.families(root))
        family = next((name for name in ("Microsoft YaHei UI", "PingFang SC", "Noto Sans CJK SC", "Noto Sans CJK TC", "WenQuanYi Micro Hei", "Arial") if name in families), "TkDefaultFont")
        self.unit = max(6, int(round(font.nametofont("TkDefaultFont").metrics("linespace") / 2.5)))
        system_size = int(font.nametofont("TkDefaultFont").cget("size"))
        self.base = max(1, system_size if system_size > 0 else int(round(abs(system_size) * 72 / max(1, root.winfo_fpixels("1i")))))
        self.font = font.Font(root=root, family=family, size=self.base)
        self.small_font = font.Font(root=root, family=family, size=max(9, self.base - 1))
        self.heading_font = font.Font(root=root, family=family, size=self.base + 10, weight="bold")
        self.brand_font = font.Font(root=root, family=family, size=self.base + 16, weight="bold")
        self.bold_font = font.Font(root=root, family=family, size=self.base, weight="bold")
        self.root.title(APP_NAME)
        self.root.configure(bg=self.palette["bg"])
        self.root.option_add("*Font", self.font)
        screen_width, screen_height = root.winfo_screenwidth(), root.winfo_screenheight()
        width = min(screen_width, max(680, int(screen_width * .88)))
        height = min(screen_height, max(500, screen_height - self.font.metrics("linespace") * 2))
        self.root.geometry(f"{width}x{height}+{max(0, (screen_width-width)//2)}+{max(0, (screen_height-height)//2)}")
        self.root.minsize(min(420, screen_width), min(380, screen_height))
        self.style = ttk.Style(root)
        if "clam" in self.style.theme_names():
            self.style.theme_use("clam")
        self.style.configure("Yuan.Horizontal.TProgressbar", troughcolor=self.palette["field"], background=self.palette["accent"], bordercolor=self.palette["field"], lightcolor=self.palette["accent"], darkcolor=self.palette["accent"], thickness=3)
        self.style.configure("Yuan.Vertical.TScrollbar", background=self.palette["border"], troughcolor=self.palette["bg"], bordercolor=self.palette["bg"], arrowcolor=self.palette["muted"], width=9)
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)
        self.viewport = tk.Canvas(root, bg=self.palette["bg"], highlightthickness=0, bd=0)
        self.viewport.grid(row=0, column=0, sticky="nsew")
        self.scrollbar = ttk.Scrollbar(root, orient="vertical", style="Yuan.Vertical.TScrollbar", command=self.viewport.yview)
        self.viewport.configure(yscrollcommand=self.scrollbar.set)
        self.body = tk.Frame(self.viewport, bg=self.palette["bg"], padx=self.unit*2, pady=self.unit)
        self.body_window = self.viewport.create_window(0, 0, window=self.body, anchor="nw")
        self.body.grid_columnconfigure(0, weight=1)
        self.viewport.bind("<Configure>", self.resize)
        self.body.bind("<Configure>", self.measure)
        self.root.bind_all("<MouseWheel>", self.wheel)
        self.root.bind_all("<Button-4>", self.wheel)
        self.root.bind_all("<Button-5>", self.wheel)
        self.bound = []
        self.build_header()
        self.build_controls()
        self.build_hero()
        self.build_cards()
        self.build_footer()
        self.build_activity()
        self.root.protocol("WM_DELETE_WINDOW", self.close)
        self.root.bind("<Control-Return>", lambda event: self.toggle())
        self.root.report_callback_exception = self.callback_error
        self.localize()
        self.refresh()
        self.root.after(max(1, int(math.sqrt(tick()) * 1000)), self.pump)
        self.root.after(max(1, int(tick() * 1000)), self.animate)

    def label(self, parent, text="", key=None, **options):
        defaults = dict(bg=parent.cget("bg"), fg=self.palette["text"], anchor="w", justify="left", bd=0, font=self.font)
        defaults.update(options)
        widget = self.tk.Label(parent, text=text if key is None else tr(key, self.language), **defaults)
        if key:
            self.bound.append((widget, key))
        return widget

    def button(self, parent, command, text="", key=None, primary=False, **options):
        defaults = dict(bg=self.palette["accent"] if primary else self.palette["panel"], fg=self.palette["bg"] if primary else self.palette["text"],
                        activebackground=self.palette["lavender"] if primary else self.palette["border"], activeforeground=self.palette["bg"] if primary else self.palette["text"],
                        disabledforeground=self.palette["muted"], relief="flat", bd=0, padx=self.unit*2, pady=self.unit, cursor="hand2", font=self.bold_font,
                        highlightthickness=1, highlightbackground=self.palette["border"], highlightcolor=self.palette["accent"], takefocus=True)
        defaults.update(options)
        widget = self.tk.Button(parent, text=text if key is None else tr(key, self.language), command=command, **defaults)
        if key:
            self.bound.append((widget, key))
        return widget

    def build_header(self):
        top = self.tk.Frame(self.body, bg=self.palette["bg"])
        top.grid(row=0, column=0, sticky="ew", pady=(0, self.unit))
        top.grid_columnconfigure(1, weight=1)
        brand = self.label(top, text="Y U A N", font=self.brand_font)
        native = self.label(top, text="NATIVE AUDIO", fg=self.palette["accent"], font=self.small_font)
        subtitle = self.label(top, key="subtitle", fg=self.palette["muted"], font=self.small_font)
        self.language_button = self.button(top, self.switch_language, text="中文  /  EN", padx=self.unit, pady=max(3, self.unit//2))
        def layout(event):
            available = event.width - brand.winfo_reqwidth() - self.language_button.winfo_reqwidth() - self.unit*4
            compact = available < self.small_font.measure("Prompt + processed audio")
            brand.grid(row=0, column=0, rowspan=1 if compact else 2, sticky="w")
            self.language_button.grid(row=0, column=2, rowspan=1 if compact else 2, sticky="e")
            native.grid(row=1 if compact else 0, column=0 if compact else 1, columnspan=3 if compact else 1, sticky="w",
                        padx=0 if compact else self.unit*2, pady=(self.unit//2 if compact else 0, 0))
            subtitle.grid(row=2 if compact else 1, column=0 if compact else 1, columnspan=3 if compact else 1, sticky="ew",
                          padx=0 if compact else self.unit*2)
            subtitle.configure(wraplength=max(self.unit, event.width if compact else available))
        top.bind("<Configure>", layout)
        layout(types.SimpleNamespace(width=self.root.winfo_width()))

    def build_controls(self):
        panel = self.tk.Frame(self.body, bg=self.palette["panel"], padx=self.unit, pady=self.unit, highlightthickness=1, highlightbackground=self.palette["border"])
        panel.grid(row=1, column=0, sticky="ew")
        panel.grid_columnconfigure(1, weight=1)
        self.label(panel, key="folder", fg=self.palette["muted"], font=self.small_font).grid(row=0, column=0, sticky="w", padx=(0, self.unit))
        self.folder_var = self.tk.StringVar(value=tr("empty_folder", self.language))
        self.folder_entry = self.tk.Entry(panel, textvariable=self.folder_var, state="readonly", readonlybackground=self.palette["panel"], fg=self.palette["text"], relief="flat", bd=0, highlightthickness=0)
        self.folder_entry.grid(row=0, column=1, sticky="ew", padx=self.unit)
        self.folder_entry.configure(width=1)
        self.choose_button = self.button(panel, self.choose, key="choose", pady=max(3, self.unit//2))
        self.choose_button.grid(row=0, column=2, sticky="e")
        self.label(panel, key="prompt", fg=self.palette["muted"], font=self.small_font).grid(row=1, column=0, sticky="nw", pady=(self.unit, 0))
        self.prompt = self.tk.Text(panel, height=2, width=1, wrap="word", undo=True, maxundo=max(1, os.cpu_count() or 1), bg=self.palette["field"], fg=self.palette["text"],
                                   insertbackground=self.palette["accent"], selectbackground=self.palette["border"], relief="flat", bd=0, padx=self.unit, pady=self.unit//2,
                                   highlightthickness=1, highlightbackground=self.palette["border"], highlightcolor=self.palette["accent"])
        self.prompt.grid(row=1, column=1, columnspan=2, sticky="ew", padx=(self.unit, 0), pady=(self.unit, 0))
        self.prompt.insert("1.0", tr("default_prompt", self.language))
        self.prompt.bind("<<Modified>>", self.prompt_changed)
        self.prompt.bind("<Tab>", lambda event: self.focus_next(event, False))
        self.prompt.bind("<Shift-Tab>", lambda event: self.focus_next(event, True))
        self.prompt.edit_modified(False)
        actions = self.tk.Frame(panel, bg=self.palette["panel"])
        actions.grid(row=2, column=0, columnspan=2, sticky="w", pady=(self.unit, 0))
        self.retry_button = self.button(actions, self.recheck, key="recheck", pady=max(3, self.unit // 2))
        self.retry_button.grid(row=0, column=0, sticky="w")
        self.fault_button = self.button(actions, self.show_fault, key="open_fault", pady=max(3, self.unit // 2))
        self.fault_button.grid(row=0, column=1, sticky="w", padx=(self.unit, 0))
        self.fault_button.grid_remove()
        self.learning_button = self.button(panel, self.show_learning, key="learning_setup", pady=max(3, self.unit // 2))
        if os.environ.get("YUAN_PUBLISHER") == "1":
            self.learning_button.grid(row=2, column=2, sticky="e", pady=(self.unit, 0))

    def build_hero(self):
        self.hero = self.tk.Frame(self.body, bg=self.palette["bg"], pady=self.unit)
        self.hero.grid(row=2, column=0, sticky="ew")
        self.hero.grid_columnconfigure(1, weight=1)
        self.orb = self.tk.Canvas(self.hero, width=self.unit*7, height=self.unit*7, bg=self.palette["bg"], highlightthickness=0)
        self.orb.grid(row=0, column=0, rowspan=3, sticky="w", padx=(0, self.unit))
        self.status_label = self.label(self.hero, font=self.heading_font)
        self.status_label.grid(row=0, column=1, sticky="sw")
        self.status_detail = self.label(self.hero, key="full_duplex", fg=self.palette["muted"], font=self.small_font)
        self.status_detail.grid(row=1, column=1, sticky="new", pady=(self.unit//2, 0))
        self.model_blocker_label = self.label(self.hero, fg=self.palette["warning"], font=self.bold_font)
        self.model_blocker_label.grid(row=2, column=1, sticky="ew", pady=(self.unit//2, 0))
        self.model_blocker_label.grid_remove()
        self.action = self.button(self.hero, self.toggle, key="start", primary=True, padx=self.unit*3, pady=self.unit*2)
        self.action.grid(row=0, column=2, rowspan=3, sticky="e", padx=(self.unit, 0))
        def layout(event):
            available = event.width - self.orb.winfo_reqwidth() - self.action.winfo_reqwidth() - self.unit*2
            compact = available < self.heading_font.measure(tr("ended", self.language))
            self.action.grid_configure(row=3 if compact else 0, column=0 if compact else 2, columnspan=3 if compact else 1,
                                       rowspan=1 if compact else 3, sticky="ew" if compact else "e", padx=0 if compact else (self.unit, 0), pady=(self.unit, 0) if compact else 0)
            for widget in (self.status_label, self.status_detail, self.model_blocker_label):
                if widget is not self.model_blocker_label or widget.cget("text"):
                    widget.grid_configure(columnspan=2 if compact else 1)
                widget.configure(wraplength=max(self.unit, event.width-self.orb.winfo_reqwidth()-self.unit if compact else available))
        self.hero.bind("<Configure>", layout)
        progress_frame = self.tk.Frame(self.body, bg=self.palette["bg"])
        progress_frame.grid(row=3, column=0, sticky="ew", pady=(0, self.unit))
        progress_frame.grid_columnconfigure(0, weight=1)
        track = self.tk.Frame(progress_frame, bg=self.palette["bg"], height=max(2, self.unit//3))
        track.grid(row=0, column=0, sticky="ew")
        track.pack_propagate(False)
        self.progress = self.ttk.Progressbar(track, style="Yuan.Horizontal.TProgressbar", maximum=1.0)
        track.configure(height=max(self.progress.winfo_reqheight(), math.ceil(self.unit / 2)))
        self.progress.pack(fill="both", expand=True)
        self.progress_label = self.label(progress_frame, text="", fg=self.palette["muted"], font=self.small_font)
        self.progress_label.grid(row=1, column=0, sticky="ew", pady=(self.unit//2, 0))
        progress_frame.bind("<Configure>", lambda event: self.progress_label.configure(wraplength=max(self.unit*4, event.width)))

    def build_cards(self):
        self.cards_frame = self.tk.Frame(self.body, bg=self.palette["bg"])
        self.cards_frame.grid(row=4, column=0, sticky="nsew")
        self.cards = {}
        self.card_rows = {}
        self.card_columns = 0
        for key in ("devices", "model", "audio", "learning", "storage", "resources"):
            frame = self.tk.Frame(self.cards_frame, bg=self.palette["panel"], highlightthickness=1, highlightbackground=self.palette["border"], padx=self.unit, pady=max(3, self.unit//2))
            frame.grid_columnconfigure(0, weight=1)
            self.label(frame, key=key, fg=self.palette["accent"] if key in ("model", "learning") else self.palette["lavender"], font=self.bold_font).grid(row=0, column=0, sticky="w", pady=(0, self.unit//2))
            rows = []
            for index in range(4):
                widget = self.label(frame, text="—", font=self.small_font, fg=self.palette["text"] if index == 0 else self.palette["muted"])
                widget.grid(row=index+1, column=0, sticky="ew", pady=0)
                rows.append(widget)
            frame.bind("<Configure>", lambda event, labels=rows: [label.configure(wraplength=max(self.unit*4, event.width-self.unit*2)) for label in labels])
            self.cards[key] = frame
            self.card_rows[key] = rows
        self.layout_cards(3)

    def build_footer(self):
        bottom = self.tk.Frame(self.body, bg=self.palette["bg"])
        bottom.grid(row=5, column=0, sticky="ew", pady=(self.unit, 0))
        bottom.grid_columnconfigure(0, weight=1)
        self.notice = self.label(bottom, fg=self.palette["warning"], font=self.small_font)
        self.notice.grid(row=0, column=0, sticky="ew", pady=(0, self.unit//2))
        self.privacy = self.label(bottom, key="privacy", fg=self.palette["muted"], font=self.small_font)
        self.privacy.grid(row=1, column=0, sticky="ew")
        for widget in (self.notice, self.privacy):
            widget.bind("<Configure>", lambda event, target=widget: target.configure(wraplength=max(1, event.width)))
        self.dock = self.tk.Frame(self.root, bg=self.palette["panel"], padx=self.unit, pady=self.unit//2)
        self.dock.grid_columnconfigure(0, weight=1)
        self.dock_status = self.label(self.dock, fg=self.palette["text"])
        self.dock_status.grid(row=0, column=0, sticky="w")
        self.dock_action = self.button(self.dock, self.toggle, key="start", primary=True, pady=self.unit//2)
        self.dock_action.grid(row=0, column=1, sticky="e")
        self.dock.bind("<Configure>", lambda event: self.dock_status.configure(wraplength=max(self.unit, event.width-self.dock_action.winfo_reqwidth()-self.unit*3)))

    def build_activity(self):
        panel = self.tk.Frame(self.body, bg=self.palette["panel"], padx=self.unit, pady=self.unit)
        panel.grid(row=6, column=0, sticky="ew", pady=(self.unit, 0))
        panel.grid_columnconfigure(1, weight=1)
        self.label(panel, key="activity", fg=self.palette["accent"], font=self.bold_font).grid(row=0, column=0, sticky="w")
        self.button(panel, self.show_activity, key="all_activity", pady=self.unit//2).grid(row=0, column=2, sticky="e")
        self.activity_text = self.tk.Text(panel, height=1, width=1, wrap="word", bg=self.palette["field"], fg=self.palette["muted"],
                                          relief="flat", bd=0, font=self.small_font, padx=self.unit, pady=self.unit//2, state="disabled")
        self.activity_text.grid(row=0, column=1, sticky="ew", padx=self.unit)

    def record_activity(self, event):
        if event.get("type") not in ("status", "error", "notice", "deployment", "learning", "network", "recovery"):
            return
        event = {key: event[key] for key in ("at", "type", "key", "reason", "action_key") if key in event}
        self.recent_activity.append(event)
        limit = max(1, self.root.winfo_screenheight() // max(1, self.small_font.metrics("linespace")))
        while len(self.recent_activity) > limit:
            self.recent_activity.popleft()
        self.render_activity()

    def render_activity(self):
        text = self.activity_text
        text.configure(state="normal")
        text.delete("1.0", "end")
        for event in self.recent_activity:
            text.insert("end", event_text(event, self.language, compact=True))
        text.see("end")
        text.configure(state="disabled")

    def show_fault(self):
        from tkinter import messagebox
        fault = self.recovery_value.get("fault") or self.current_failure.get("fault", {})
        category = fault.get("category", "unknown")
        reason = local_message(fault.get("reason", tr("action_unknown", self.language)), self.language)
        action = tr(self.recovery_value.get("action_key", "action_" + category), self.language)
        messagebox.showwarning(APP_NAME, reason + "\n" + action, parent=self.root)

    def show_activity(self):
        if not self.launcher:
            return
        if self.activity_window and self.activity_window.winfo_exists():
            self.activity_window.lift()
            return
        from tkinter import scrolledtext
        window = self.tk.Toplevel(self.root)
        self.activity_window = window
        window.title(tr("all_activity", self.language))
        window.configure(bg=self.palette["bg"])
        window.geometry(f"{max(1, self.root.winfo_width())}x{max(1, self.root.winfo_height())}")
        bar = self.tk.Frame(window, bg=self.palette["panel"])
        bar.pack(fill="x")
        text = scrolledtext.ScrolledText(window, wrap="word", font=self.small_font, bg=self.palette["field"], fg=self.palette["text"],
                                         insertbackground=self.palette["accent"], relief="flat")
        text.pack(fill="both", expand=True)
        position = {"before": None, "trail": [], "first": None, "live": True, "last": None}
        store = self.launcher.activity
        def render():
            if not window.winfo_exists() or store.closed:
                return
            limit = max(1, window.winfo_height() // max(1, self.small_font.metrics("linespace")))
            try:
                rows = store.page(position["before"], limit)
            except (sqlite3.Error, OSError) as exc:
                rows = [(0, {"type": "error", "reason": concise_error(exc)})]
            marker = (rows[-1][0] if rows else None, position["before"], self.language)
            if marker != position["last"]:
                position["first"] = rows[0][0] if rows else None
                position["last"] = marker
                text.configure(state="normal")
                text.delete("1.0", "end")
                text.insert("1.0", "\n".join(event_text(event, self.language, compact=True) for _, event in rows))
                text.configure(state="disabled")
                if position["live"]:
                    text.see("end")
            self.root.after(max(1, int(math.sqrt(tick()) * 1000)), render)
        def earlier():
            if position["first"] is not None:
                position["trail"].append(position["before"])
                position["before"] = position["first"]
                position["live"] = False
        def later():
            position["before"] = position["trail"].pop() if position["trail"] else None
            position["live"] = position["before"] is None
        def live():
            position.update(before=None, trail=[], live=True, last=None)
        for key, command in (("older", earlier), ("newer", later), ("live", live)):
            self.button(bar, command, text=tr(key, self.language), pady=self.unit//2).pack(side="left", padx=self.unit//2, pady=self.unit//2)
        text.bind("<MouseWheel>", lambda event: position.update(live=False))
        render()

    def layout_cards(self, columns):
        if columns == self.card_columns:
            return
        for column in range(3):
            self.cards_frame.grid_columnconfigure(column, weight=1 if column < columns else 0, uniform="cards" if column < columns else "")
        for index, frame in enumerate(self.cards.values()):
            frame.grid(row=index//columns, column=index%columns, sticky="nsew", padx=(0 if index%columns == 0 else self.unit//2, 0 if index%columns == columns-1 else self.unit//2), pady=self.unit//2)
        self.card_columns = columns

    def resize(self, event):
        self.viewport.itemconfigure(self.body_window, width=event.width)
        self.layout_cards(3 if event.width >= self.font.measure("M") * 82 else 2 if event.width >= self.font.measure("M") * 56 else 1)
        self.root.after_idle(self.measure)

    def measure(self, event=None):
        self.viewport.configure(scrollregion=self.viewport.bbox("all"))
        needs_scroll = self.body.winfo_reqheight() > self.root.winfo_height()
        if needs_scroll:
            self.scrollbar.grid(row=0, column=1, sticky="ns")
            self.dock.grid(row=1, column=0, columnspan=2, sticky="ew")
        else:
            self.scrollbar.grid_remove()
            self.dock.grid_remove()
            self.viewport.yview_moveto(0)

    def wheel(self, event):
        if event.widget is self.prompt or event.widget is self.activity_text or event.widget.winfo_toplevel() is not self.root:
            return
        if self.body.winfo_reqheight() <= self.viewport.winfo_height():
            return
        delta = -1 if getattr(event, "num", None) == 4 or getattr(event, "delta", 0) > 0 else 1
        self.viewport.yview_scroll(delta, "units")

    def focus_next(self, event, reverse):
        (event.widget.tk_focusPrev() if reverse else event.widget.tk_focusNext()).focus_set()
        return "break"

    def localize(self):
        self.bound = [(widget, key) for widget, key in self.bound if widget.winfo_exists()]
        for widget, key in self.bound:
            widget.configure(text=tr(key, self.language))
        if self.workspace is None:
            self.folder_var.set(tr("empty_folder", self.language))
        self.language_button.configure(text="中文  /  EN" if self.language == "中文" else "EN  /  中文")
        self.render_activity()
        self.refresh()

    def switch_language(self):
        old_default = tr("default_prompt", self.language)
        current = self.prompt.get("1.0", "end-1c")
        self.language = "English" if self.language == "中文" else "中文"
        if not self.session_id and current == old_default:
            self.prompt.delete("1.0", "end")
            self.prompt.insert("1.0", tr("default_prompt", self.language))
        if self.launcher:
            self.launcher.send({"action": "language", "language": self.language})
        self.save_settings()
        self.localize()

    def prompt_changed(self, event=None):
        if self.prompt.edit_modified():
            self.prompt.edit_modified(False)
            if self.save_job:
                self.root.after_cancel(self.save_job)
            self.save_job = self.root.after(max(1, int(math.sqrt(tick()) * 1000)), self.save_settings)

    def save_settings(self):
        if self.save_job is not None:
            self.root.after_cancel(self.save_job)
            self.save_job = None
        if self.workspace is None:
            return True
        payload = {"prompt": self.prompt.get("1.0", "end-1c"), "language": self.language}
        if payload == self.last_saved_prompt:
            return True
        try:
            atomic_json(safe_path(self.workspace, STATE_NAME) / "settings.json", payload)
            self.last_saved_prompt = payload
            return True
        except (OSError, ValueError, YuanError) as exc:
            self.reason = concise_error(exc)
            self.refresh()
            return False

    def recheck(self):
        if self.closing or self.session_id or not self.workspace or self.launcher and self.launcher.process_retained:
            return
        if self.launcher and self.launcher.thread.is_alive():
            if self.launcher.request_recheck():
                self.notice_key = "recheck_requested"
        else:
            try:
                gate = RecoveryGate(safe_path(self.workspace, STATE_NAME), lambda *args, **kwargs: None)
                gate.reset("recheck_requested")
                self.open_workspace(self.workspace)
            except Exception as exc:
                self.reason = concise_error(exc)
        self.refresh()

    def choose(self):
        from tkinter import filedialog
        if self.session_id or self.closing or self.launcher and self.launcher.process_retained:
            return
        selected = filedialog.askdirectory(parent=self.root, title=tr("choose", self.language), mustexist=True, initialdir=str(self.workspace) if self.workspace else None)
        if not selected:
            return
        selected = Path(selected).resolve()
        if selected == self.workspace and self.launcher and self.launcher.thread.is_alive():
            return
        if not self.save_settings():
            return
        self.pending_workspace = selected
        self.ready = False
        self.runtime_ready = False
        self.readiness_values = {}
        if self.launcher and self.launcher.thread.is_alive():
            self.status_key = "closed"
            self.launcher.stop()
            self.choose_button.configure(state="disabled")
        else:
            self.open_workspace(selected)
        self.refresh()

    def open_workspace(self, workspace):
        self.pending_workspace = None
        workspace = Path(workspace).expanduser().resolve()
        if not self.save_settings():
            return
        lease = None
        try:
            state = safe_path(workspace, STATE_NAME)
            state.mkdir(parents=True, exist_ok=True)
            fd, test = tempfile.mkstemp(dir=state)
            os.close(fd)
            os.unlink(test)
            lease = WorkspaceLease(state / "workspace.lock")
            prompt = self.prompt.get("1.0", "end-1c") if self.workspace is None or workspace == self.workspace else None
            settings = workspace_settings(state, self.language, prompt)
        except (OSError, YuanError) as exc:
            if lease is not None:
                lease.close()
            self.reason = concise_error(exc)
            self.status_key = "failure"
            self.refresh()
            return
        if self.launcher and not self.launcher.thread.is_alive():
            self.launcher.activity.close()
        if self.activity_window and self.activity_window.winfo_exists():
            self.activity_window.destroy()
        for window in (self.learning_window, self.review_window):
            if window and window.winfo_exists():
                window.destroy()
        self.workspace = workspace
        self.folder_var.set(str(workspace))
        self.last_saved_prompt = None
        self.language = settings["language"]
        self.prompt.configure(state="normal")
        self.prompt.delete("1.0", "end")
        self.prompt.insert("1.0", settings["prompt"])
        self.prompt.edit_modified(False)
        self.reason = ""
        self.notice_key = ""
        self.model_values, self.device_values, self.learning_values, self.resource_values, self.audio_values, self.progress_value = {}, {}, {}, {}, {}, {}
        self.pipeline_verified = False
        self.phase_values = {}
        self.source_values = {}
        self.learning_key = "waiting_data"
        self.network_key = "wait"
        self.recent_activity.clear()
        self.activity_text.configure(state="normal")
        self.activity_text.delete("1.0", "end")
        self.activity_text.configure(state="disabled")
        self.ready = False
        self.runtime_ready = False
        self.readiness_values = {}
        self.generation += 1
        self.attempt_id = None
        self.attempt_number = 0
        self.event_sequence = 0
        self.phase_id = None
        self.supervision_value = {}
        self.current_failure = {}
        self.last_failure = {}
        self.recovery_value = {}
        self.startup_values = {}
        self.deployment_value = {}
        self.notice_context = None
        self.status_key = "setup"
        self.save_settings()
        try:
            self.launcher = Launcher(workspace, self.generation, self.mailbox, lease=lease)
        except Exception as exc:
            lease.close()
            self.launcher = None
            self.status_key = "failure"
            self.reason = concise_error(exc)
        self.choose_button.configure(state="normal")
        self.localize()

    def can_start(self):
        return bool(not self.closing and not self.ending and not self.pending_workspace and self.workspace and self.launcher
                    and self.launcher.thread.is_alive() and self.ready and can_converse(self.readiness_values)
                    and self.readiness_values.get("storage_ready") is True)

    def toggle(self):
        from tkinter import messagebox
        if self.closing or self.ending:
            return
        if self.session_id:
            if not messagebox.askyesno(tr("confirm_title", self.language), tr("confirm_stop", self.language), parent=self.root):
                return
            self.ending = True
            self.status_key = "ending"
            self.level = 0.0
            if not self.launcher or not self.launcher.send({"action": "stop", "id": self.session_id}):
                self.finish_session()
                self.notice_key = "stop_incomplete"
                self.ready = False
                self.readiness_values = {}
        elif self.can_start():
            for window in (self.learning_window, self.review_window):
                if window and window.winfo_exists():
                    window.destroy()
            value = self.prompt.get("1.0", "end-1c").strip()
            if not value:
                value = tr("default_prompt", self.language)
                self.prompt.insert("1.0", value)
                self.notice_key = "no_prompt"
            self.save_settings()
            session_id = uuid.uuid4().hex
            action = {"action": "start", "id": session_id, "prompt": value, "language": self.language}
            if self.launcher.send(action):
                self.session_id = session_id
                self.status_key = "connecting"
                self.ending = False
                self.audio_values = {"turns": 0}
                self.reason = ""
            else:
                self.notice_key = "command_failed"
        self.refresh()

    def finish_session(self):
        if self.session_started:
            self.audio_values["elapsed"] = time.monotonic() - self.session_started
        self.session_id = None
        self.session_started = None
        self.ending = False
        self.status_key = "ended"
        self.phase_values = {}
        self.device_values["continuous_active"] = False
        self.audio_values.update(continuous=False, speech_active=False, recorded=0.0)
        self.level = 0.0

    def close(self):
        from tkinter import messagebox
        if self.closing:
            return
        if self.session_id and not messagebox.askyesno(tr("confirm_title", self.language), tr("confirm_close", self.language), parent=self.root):
            return
        self.closing = True
        self.ready = False
        self.save_settings()
        self.status_key = "closed"
        if self.launcher and self.launcher.thread.is_alive():
            self.launcher.stop()
        else:
            if self.launcher:
                self.launcher.activity.close()
            self.destroy()
            return
        self.refresh()

    def destroy(self):
        self.progress.stop()
        for identifier in self.root.tk.call("after", "info"):
            self.root.tk.call("after", "cancel", identifier)
        self.root.destroy()

    def callback_error(self, kind, value, traceback):
        self.reason = concise_error(value)
        try:
            self.refresh()
        except Exception:
            pass

    def handle(self, event):
        kind = event.get("type")
        if event.get("generation") is not None and event["generation"] != self.generation:
            return
        if kind == "attempt":
            number = event.get("attempt_number", 0)
            if number <= self.attempt_number:
                return
            if self.current_failure:
                self.last_failure = dict(self.current_failure)
            self.current_failure = {}
            self.recovery_value = {}
            self.startup_values = {}
            self.deployment_value = {}
            self.notice_context = None
            self.attempt_id = event.get("attempt_id")
            self.attempt_number = number
            self.event_sequence = 0
            self.phase_id = None
            self.supervision_value = {}
            self.reason = self.notice_key = ""
            self.ready = self.runtime_ready = self.pipeline_verified = False
            self.readiness_values = {}
            self.progress_value = self.phase_values = {}
            self.model_values = {}
            self.device_values = {}
            self.resource_values = {}
            self.learning_values = {}
            self.learning_key = "waiting_data"
            self.network_key = "wait"
            self.source_values = {}
            if self.session_id:
                self.finish_session()
                self.notice_key = "stop_incomplete"
            if not self.closing:
                self.status_key = "setup"
        elif event.get("attempt_id") is not None and event["attempt_id"] != self.attempt_id:
            return
        sequence = event.get("event_sequence")
        if isinstance(sequence, int):
            if sequence <= self.event_sequence:
                return
            self.event_sequence = sequence
        self.record_activity(event)
        if kind in ("status", "started", "ended", "audio", "turn") and event.get("id") is not None and event["id"] != self.session_id:
            return
        if kind == "status":
            phase_id = event.get("phase_id")
            if phase_id != self.phase_id or event.get("key") != self.status_key:
                self.progress_value = {}
                self.phase_values = {}
                self.supervision_value = {}
            self.phase_id = phase_id
            if self.current_failure and event.get("key") not in ("retry", "failure", "startup_blocked", "network_wait"):
                self.last_failure = dict(self.current_failure)
                self.current_failure = {}
                self.notice_key = self.reason = ""
            if not self.ending and not self.closing:
                self.status_key = event.get("key", self.status_key)
        elif kind == "recovery":
            self.recovery_value = dict(event)
            if event.get("blocked"):
                self.progress_value = {}
                self.phase_values = {}
                self.supervision_value = {}
        elif kind == "startup_resources":
            self.startup_values = dict(event)
        elif kind == "runtime_profile":
            self.startup_values.update({key: event[key] for key in ("executable", "python", "machine") if key in event})
        elif kind == "deployment_preflight":
            self.deployment_value = dict(event)
        elif kind == "download_complete":
            context = (event.get("attempt_id"), event.get("phase_id"), event.get("artifact_id"))
            if self.notice_context == context and not self.current_failure:
                self.notice_key = self.reason = ""
                self.notice_context = None
        elif kind == "process_reaped" and event.get("scope") == "runtime_probe":
            self.startup_values.update(process_running=False, pid=None)
        elif kind == "supervision":
            self.supervision_value = {} if event.get("complete") else {**event, "received_at": time.monotonic()}
        elif kind == "progress":
            if event.get("phase_id") != self.phase_id:
                return
            if not event.get("phase_id") and event.get("key") != self.status_key:
                return
            self.progress_value = event
        elif kind == "source_progress":
            self.source_values = event
        elif kind == "phase":
            if event.get("phase_id") != self.phase_id:
                return
            self.phase_values = event
        elif kind == "ready":
            if self.closing:
                return
            self.model_values.update({key: event[key] for key in ("capability", "acoustic_ready", "dialogue_trained", "dialogue_ready", "playback_ready", "conversation_verified") if key in event})
            self.model_values.setdefault("capability", "cold_start")
            self.runtime_ready = True
            self.status_key = "preparing_model"
            self.progress_value = {}
            self.phase_values = {}
        elif kind == "readiness":
            self.readiness_values = dict(event)
            self.runtime_ready = bool(event.get("runtime_ready"))
            self.ready = bool(can_converse(event) and event.get("storage_ready") is True and event.get("conversation_ready") is True and not self.closing)
            if event.get("capability"):
                self.model_values["capability"] = event["capability"]
            if not self.session_id and not self.closing and not self.ending:
                self.status_key = "ready" if self.ready else "mic_missing" if self.runtime_ready and not event.get("audio_ready") else event.get("model_blocker") or "preparing_model" if self.runtime_ready else self.status_key
            if self.ready and self.notice_key in ("start_rejected", "mic_missing", "release_invalid", "owned_release_missing"):
                self.notice_key = self.reason = ""
        elif kind == "review_snapshot":
            self.show_review(event)
        elif kind == "storage_state":
            self.resource_values["storage"] = dict(event)
        elif kind == "pipeline":
            self.pipeline_verified = bool(event.get("verified"))
            if "conversation_verified" in event:
                self.model_values["conversation_verified"] = bool(event.get("conversation_verified"))
        elif kind == "started":
            if event.get("id") != self.session_id:
                return
            self.learning_key = "learn_yield"
            self.phase_values = {}
            self.session_started = time.monotonic()
            self.device_values["continuous_active"] = bool(event.get("continuous"))
            if not self.ending and not self.closing:
                self.status_key = "listening" if event.get("continuous") else "connecting"
        elif kind == "ended":
            if event.get("id") == self.session_id:
                self.finish_session()
                if event.get("storage_complete") is False or event.get("capture_complete") is False:
                    self.notice_key = "stop_incomplete"
                    self.audio_values["storage_complete"] = False
        elif kind == "devices":
            self.device_values.update(event)
        elif kind == "model":
            self.model_values.update(event)
        elif kind in ("audio", "turn"):
            if kind == "turn" and event.get("id") != self.session_id:
                return
            self.audio_values.update(event)
            if kind == "turn":
                self.phase_values = {}
            if event.get("continuous") is not None:
                self.device_values["continuous_active"] = bool(event["continuous"])
            if not self.ending:
                self.level = max(0.0, min(1.0, float(event.get("level", 0.0))))
        elif kind == "generation_end":
            self.audio_values["generation_end"] = event.get("key")
        elif kind == "learning":
            self.learning_values.update(event)
            self.learning_key = event.get("key", self.learning_key)
            if self.phase_values.get("key") in ("validating", "checkpointing") and self.learning_key not in ("validating",):
                self.phase_values = {}
            for key in ("accepted", "acoustic_ready", "dialogue_trained", "dialogue_ready", "playback_ready", "capability"):
                if key in event:
                    self.model_values[key] = event[key]
            if "capability" not in event:
                self.model_values["capability"] = "dialogue_ready" if self.model_values.get("playback_ready") and self.model_values.get("dialogue_ready") else "release_wait" if self.model_values.get("dialogue_trained") else "acoustic_only" if self.model_values.get("acoustic_ready") else "cold_start"
            if event.get("key") == "promoted" and self.notice_key == "collecting":
                self.notice_key = ""
            if event.get("reason"):
                self.reason = event["reason"]
        elif kind == "checkpoint_saved":
            if self.phase_values.get("key") == "checkpointing":
                self.phase_values = {}
        elif kind == "network":
            self.network_key = event.get("key", self.network_key)
            if self.network_key == "available" and self.phase_values.get("key") == "network":
                self.phase_values = {}
            if event.get("reason"):
                self.reason = event["reason"]
        elif kind == "metrics":
            self.resource_values.update(event)
            capture = event.get("capture")
            if isinstance(capture, dict) and capture.get("id") == self.session_id:
                self.audio_values.update(capture)
                self.device_values["continuous_active"] = bool(capture.get("continuous"))
        elif kind in ("notice", "error"):
            self.notice_context = (event.get("attempt_id"), event.get("phase_id"), event.get("artifact_id")) if event.get("component") == "runtime_download" and event.get("artifact_id") else None
            self.notice_key = event.get("key", "")
            self.reason = event.get("reason") or ""
            if kind == "error":
                self.current_failure = dict(event)
                self.ready = self.runtime_ready = self.pipeline_verified = False
                self.readiness_values = {}
                self.progress_value = {}
                self.phase_values = {}
                self.supervision_value = {}
                self.status_key = "failure"
        elif kind in ("exit", "disconnected", "process_reaped"):
            self.runtime_ready = False
            self.readiness_values = {}
            self.ready = False
            self.pipeline_verified = False
            self.device_values["continuous_active"] = False
            self.audio_values.update(continuous=False, speech_active=False, level=0.0)
            self.level = 0.0
            self.phase_values = {}
            self.progress_value = {}
            self.supervision_value = {}
            if self.session_id:
                self.finish_session()
                self.notice_key = "stop_incomplete"
            if not self.closing:
                self.status_key = "retry"
        elif kind == "launcher_closed":
            if event.get("process_retained"):
                self.closing = False
                self.pending_workspace = None
                self.status_key = "failure"
                self.choose_button.configure(state="disabled")
                return
            if self.closing:
                if self.launcher:
                    self.launcher.activity.close()
                self.destroy()
                return
            if self.pending_workspace:
                if self.launcher and self.launcher.thread.is_alive():
                    self.root.after(max(1, int(math.sqrt(tick()) * 1000)), lambda: self.handle({"type": "launcher_closed"}))
                    return
                self.open_workspace(self.pending_workspace)
            self.choose_button.configure(state="normal")

    def card(self, key, lines):
        for widget, text in zip(self.card_rows[key], lines):
            widget.configure(text=str(text))

    def refresh(self):
        language = self.language
        translate = lambda key: tr(key, language)
        status = translate(self.status_key) if self.workspace else translate("title")
        self.status_label.configure(text=status)
        self.dock_status.configure(text=status)
        flags = self.readiness_values
        readiness = " · ".join(("✓ " if flags.get(key) else "○ ") + translate(label) for key, label in (("runtime_ready", "environment"), ("pipeline_ready", "pipeline_ok"), ("audio_ready", "devices"), ("model_ready", "acceptance"), ("storage_ready", "storage_health"), ("learning_ready", "learning_ready")))
        detail = translate("empty_folder") if self.workspace is None else readiness + "\n" + translate(self.model_values.get("capability", "cold_start")) if self.runtime_ready else translate("subtitle")
        blocker = ""
        if self.workspace and not self.runtime_ready and self.deployment_value:
            if not self.deployment_value.get("source_configured"):
                blocker = translate(self.deployment_value.get("key", "owned_source_absent"))
            else:
                detail += "\n" + translate(self.deployment_value.get("key", "owned_source_found"))
        learning_state = flags.get("learning", {})
        if self.runtime_ready and not flags.get("model_ready"):
            if flags.get("model_blocker") in ("native_learning", "dialogue_data_missing"):
                detail = readiness
                blocker = translate("dialogue_data_missing" if flags.get("model_blocker") == "dialogue_data_missing" else "native_unverified")
            if flags.get("model_detail"):
                detail += "\n" + local_message(flags["model_detail"], language)
            if os.environ.get("YUAN_PUBLISHER") == "1":
                missing = learning_state.get("missing", [])
                if missing:
                    detail += "\n" + translate("missing_data") + ": " + "; ".join(f"{row['language']} {row['split']} {row['have']}/{row['need']}" for row in missing)
                if learning_state.get("review_pending"):
                    detail += "\n" + translate("review_open")
            else:
                missing = learning_state.get("missing", [])
                if missing and flags.get("model_blocker") != "dialogue_data_missing":
                    detail += "\n" + translate("native_data_needed")
                elif not missing and learning_state.get("review_pending"):
                    detail += "\n" + translate("release_wait")
        self.model_blocker_label.configure(text=blocker)
        if blocker:
            self.model_blocker_label.grid()
        else:
            self.model_blocker_label.grid_remove()
        recovering = bool(self.launcher and self.launcher.blocked.is_set())
        retained = bool(self.launcher and self.launcher.process_retained)
        can_recheck = bool(self.workspace and not self.closing and not self.session_id and not retained and
                           (recovering or self.launcher is None or not self.launcher.thread.is_alive()))
        self.retry_button.configure(state="normal" if can_recheck else "disabled")
        if can_recheck:
            self.retry_button.grid()
            self.retry_button.master.grid()
        else:
            self.retry_button.grid_remove()
            if not self.current_failure:
                self.retry_button.master.grid_remove()
        if recovering and self.recovery_value:
            recovery = self.recovery_value
            fault = recovery.get("fault", {})
            detail = translate("fault_" + fault.get("category", "unknown"))
            if fault.get("stage"):
                detail += " · " + translate(fault["stage"])
            reason = local_message(fault.get("reason", ""), language).split("\n", 1)[0][:240]
            if reason:
                detail += " · " + reason
            detail += "\n" + translate("recovery_action") + " · " + translate(recovery.get("action_key", "action_unknown"))
            if recovery.get("retry_blocker"):
                detail += "\n" + translate(recovery["retry_blocker"])
            detail += "\n" + translate("recovery_attempts") + f" {recovery.get('matching_failures', 0)}/{recovery.get('same_fault_limit', '—')} · " + translate("campaign_remaining") + " " + human_duration(recovery.get("remaining_seconds", 0))
            if recovery.get("transfer_remaining_seconds") is not None:
                detail += " · " + translate("download_campaign_remaining") + " " + human_duration(recovery["transfer_remaining_seconds"])
        if recovering and self.recovery_value.get("fault"):
            self.fault_button.master.grid()
            self.fault_button.grid()
        else:
            self.fault_button.grid_remove()
        self.learning_button.configure(state="normal" if self.runtime_ready and self.workspace and not self.session_id and not self.closing else "disabled")
        self.status_detail.configure(text=detail, fg=self.palette["accent"] if self.ready else self.palette["warning"] if self.runtime_ready else self.palette["muted"])
        button_key = "ending" if self.ending else "stop" if self.session_id else "start"
        enabled = not self.closing and not self.ending and bool(self.session_id or self.can_start())
        for button in (self.action, self.dock_action):
            button.configure(text=translate(button_key), state="normal" if enabled else "disabled", bg=(self.palette["warning"] if self.session_id else self.palette["accent"]) if enabled else self.palette["border"])
        self.prompt.configure(state="disabled" if self.session_id or self.closing else "normal")
        self.choose_button.configure(state="disabled" if self.session_id or self.closing or self.pending_workspace or retained else "normal")
        devices = self.device_values
        self.card("devices", [translate("input") + " · " + str(devices.get("input", "—")), translate("output") + " · " + str(devices.get("output", "—")),
                              f"{devices.get('input_rate', '—')} Hz → {devices.get('output_rate', '—')} Hz",
                              ("● " if devices.get("continuous_active") and self.session_id else "○ ") + translate("full_duplex")])
        model = self.model_values
        acoustic_state = ("✓ 声学" if language == "中文" else "✓ Acoustics") if model.get("acoustic_ready") else ("○ 声学" if language == "中文" else "○ Acoustics")
        dialogue_state = ("✓ 听测" if language == "中文" else "✓ Review") if model.get("dialogue_ready") else ("○ 听测" if language == "中文" else "○ Review")
        playback_state = ("✓ 播放" if language == "中文" else "✓ Play") if model.get("playback_ready") else ("○ 播放" if language == "中文" else "○ Play")
        pipeline_state = ("✓" if self.pipeline_verified else "○") + (" 链路" if language == "中文" else " Pipeline")
        self.card("model", [model.get("name", "Yuan Native Audio"), f"{model.get('device', '—')} · {model.get('dtype', '—')} · {model.get('size', '—')}",
                            model.get("architecture", "prompt + audio → audio") + "\n" + translate("serving_model") + " · " + str(flags.get("serving_model") or "—")[:12] + " / " + translate("candidate_model") + " · " + str(flags.get("learning_model") or "—")[:12],
                            f"{pipeline_state} · {acoustic_state} · {dialogue_state} · {playback_state}"])
        audio = self.audio_values
        elapsed = time.monotonic() - self.session_started if self.session_started else audio.get("elapsed", 0.0)
        latency = audio.get("latency")
        self.card("audio", [f"{audio.get('turns', 0)} {translate('turns')} · {int(elapsed)//60:02}:{int(elapsed)%60:02}",
                            f"{translate('input')} {float(audio.get('input_seconds', audio.get('recorded', 0))):.1f}s · {translate('output')} {float(audio.get('output_seconds', 0)):.1f}s",
                            ("响应 " if language == "中文" else "Response ") + (f"{latency:.2f}s" if latency is not None else "—") + (" · " + translate(audio["generation_end"]) if audio.get("generation_end") else ""),
                            f"{translate('input_faults')} {audio.get('input_faults', 0)} · {translate('output_faults')} {audio.get('output_faults', 0)} · {translate('pending')} {audio.get('pending', 0)} · {translate('storage_queue')} {audio.get('pending_storage', 0)}"])
        learning = self.learning_values
        learning_label = self.learning_key if self.model_values else "native_bootstrap" if self.workspace else "idle"
        self.card("learning", [translate(learning_label),
                               f"{translate('candidate_progress')} {learning_state.get('candidate_improvements', learning.get('accepted', 0))}",
                               f"{translate('serving_versions')} {learning_state.get('serving_versions', 0)}",
                               translate("contrast_pending") if learning_state.get("review_pending") else translate("candidate_isolated") if learning_state.get("candidate_isolated") else f"{translate('steps')} {learning.get('steps', 0)}"])
        resources = self.resource_values
        samples = resources.get("samples", {})
        local, public, dialogue = samples.get("local", {}), samples.get("public", {}), samples.get("dialogue", {})
        dataset = samples.get("dataset", {})
        learnable = sum(values.get("learnable", 0) for values in (local, public, dataset))
        excluded = sum(values.get("excluded", 0) for values in (local, public, dataset))
        pair_text = ("已标注配对 " if language == "中文" else "Annotated pairs ") + f"{dialogue.get('verified', 0)}/{dialogue.get('count', 0)} · T/V/G/R {dialogue.get('train', 0)}/{dialogue.get('validation', 0)}/{dialogue.get('guard', 0)}/{dialogue.get('release', 0)}"
        source = self.source_values
        network_detail = translate(self.network_key)
        if source and self.network_key not in ("offline", "source_paused"):
            network_detail += " · " + human_bytes(source.get("current", 0)) + " / " + (human_bytes(source["total"]) if source.get("total") else translate("unknown_total"))
        storage = resources.get("storage", {})
        space = resources.get("storage_budget", {})
        storage_line = (translate("reserved_storage") + " " + human_bytes(space.get("reserved_bytes", 0)) + " · ") + f"{translate('saved')} {storage.get('saved', 0)} · {translate('pending')} {storage.get('pending', 0)} · {translate('lost')} {storage.get('lost_seconds', 0):.1f}s"
        if storage.get("retrying"):
            storage_line += " · " + translate("storage_retry")
        storage_details = (("可学习 " if language == "中文" else "Learnable ") + str(learnable) + " · " + translate("excluded") + " " + str(excluded) + " · " + pair_text + "\n") if os.environ.get("YUAN_PUBLISHER") == "1" else ""
        self.card("storage", [f"{translate('local')} {local.get('count', 0)} · {float(local.get('seconds', 0))/60:.1f} min",
                              f"{translate('public')} {public.get('count', 0)} · {float(public.get('seconds', 0))/60:.1f} min",
                              network_detail, storage_details + storage_line])
        self.card("resources", [f"CPU {resources.get('cpu', 0):.1f}% · Yuan {human_bytes(resources.get('process_ram', 0))}",
                                "RAM " + translate("available") + " " + human_bytes(resources.get("ram_available", 0)),
                                "GPU " + human_bytes(resources["gpu_used"]) + " / " + human_bytes(resources["gpu_total"]) if "gpu_used" in resources else "GPU —",
                                "Disk " + human_bytes(resources.get("disk_free", 0)) + " / " + human_bytes(resources.get("disk_total", 0))] if resources else ["—"] * len(self.card_rows["resources"]))
        if not self.runtime_ready and self.workspace:
            startup = self.startup_values
            running = startup.get("process_running") and not recovering
            task_active = startup.get("task_running") and not recovering
            task = translate("process_running" if running else "no_startup_task" if recovering else "task_running" if task_active else "process_idle")
            if task_active and not running and startup.get("task_kind"):
                task += " · " + translate(startup["task_kind"])
            if running and startup.get("pid"):
                task += " · PID " + str(startup["pid"])
            activity = startup.get("process_activity")
            activity_text = (f"CPU {activity[0]:.2f}s · I/O {human_bytes(activity[1])} / {human_bytes(activity[2])}" if activity else translate("monitor_unknown") if running else translate("not_measured"))
            disk_text = "Disk " + human_bytes(startup["disk_free"]) + " / " + human_bytes(startup["disk_total"]) if startup.get("disk_free") is not None else "Disk · " + translate("not_measured")
            executable = startup.get("executable")
            interpreter = Path(executable).name if executable else translate("not_measured")
            if not running:
                interpreter += " · " + translate("engine_waiting")
            self.card("resources", [task, interpreter, activity_text, disk_text])
        supervised = {} if recovering else self.supervision_value
        progress = {} if recovering else self.phase_values if self.phase_values.get("total") else self.progress_value
        if supervised:
            progress = {}
        batch = "overall_current" in progress
        total = progress.get("overall_total") if batch else progress.get("total")
        current = progress.get("overall_current", 0) if batch else progress.get("current", 0)
        if total:
            self.progress.stop()
            self.progress.configure(mode="determinate", value=max(0, min(1, current/total)))
            suffix = f" · {human_bytes(current)} / {human_bytes(total)}" if progress.get("unit", "bytes") == "bytes" else f" · {current} / {total}"
        else:
            suffix = ""
            if self.workspace and (supervised or not self.runtime_ready) and not self.closing and self.status_key not in ("failure", "retry", "startup_blocked", "network_wait"):
                if self.progress.cget("mode") != "indeterminate":
                    self.progress.configure(mode="indeterminate")
                    self.progress.start(max(1, int(math.sqrt(tick()) * 1000)))
            else:
                self.progress.stop()
                self.progress.configure(mode="determinate", value=1.0 if self.ready and can_converse(self.readiness_values) else 0.0)
        detail = str(progress.get("detail", ""))
        if self.phase_values and (self.runtime_ready or self.status_key in ("checking", "loading")):
            phase = self.phase_values
            detail = translate(phase.get("key", "wait"))
            if "current" in phase:
                detail += f" · {phase['current']} / {phase.get('total', '—')} {phase.get('unit', '')}"
            suffix = ""
        if not total and progress.get("unit", "bytes") == "bytes" and progress.get("current"):
            suffix = " · " + human_bytes(current) + " / " + translate("unknown_total")
        if progress.get("file"):
            detail = str(progress["file"]) + (" · " + detail if detail and detail != progress["file"] else "")
        if batch:
            overall = translate("download_overall") + " · " + human_bytes(current) + " / " + (human_bytes(total) if total is not None else translate("unknown_total"))
            overall += " · " + translate("download_files") + f" {progress.get('files_completed', 0)}/{progress.get('file_count', 0)}"
            if progress.get("active_files"):
                overall += " · " + translate("download_parallel") + " " + str(progress["active_files"])
            if progress.get("overall_speed") is not None:
                overall += " · " + translate("download_speed") + " " + human_bytes(progress["overall_speed"]) + "/s"
            if progress.get("files_completed", 0) < progress.get("file_count", 0):
                overall += " · " + (translate("download_estimate") + " ≈ " + human_duration(progress["eta_seconds"]) if progress.get("eta_seconds") is not None else translate("download_estimate_unknown"))
            detail = overall + "\n" + translate("download_current") + " · " + detail
            suffix = " · " + human_bytes(progress.get("current", 0)) + " / " + (human_bytes(progress["total"]) if progress.get("total") is not None else translate("unknown_total"))
        elif progress.get("file_count"):
            suffix += f" · {progress.get('file_index', 0)} / {progress['file_count']}"
        if progress.get("transfer_state"):
            suffix += " · " + translate(progress["transfer_state"])
        if progress.get("source"):
            suffix += " · " + str(progress["source"])
        if progress.get("download_attempt"):
            suffix += " · " + translate("download_attempts") + f" {progress['download_attempt']}/{progress.get('download_attempts', '—')}"
        if progress.get("connect_seconds") is not None and progress.get("transfer_state") == "download_connecting":
            suffix += " · " + translate("download_connection_budget") + f" {progress['connect_seconds']:.1f}s"
        if not batch and progress.get("speed") is not None:
            suffix += " · " + human_bytes(progress["speed"]) + "/s"
        if progress.get("last_progress_at") and not self.runtime_ready:
            suffix += " · " + translate("last_progress") + f" {max(0.0, time.time() - progress['last_progress_at']):.1f}s"
        if progress.get("remaining_seconds") is not None:
            suffix += " · " + translate("remaining") + f" {max(0.0, progress['remaining_seconds']):.1f}s"
        if progress.get("monitor_available") is False:
            suffix += " · " + translate("monitor_unknown")
        if self.launcher and not self.runtime_ready:
            if recovering:
                mode = self.recovery_value.get("mode")
                detail = translate("retry_next_action" if mode == "automatic_retry" else "waiting_conditions" if mode == "waiting_conditions" else "no_startup_task")
                suffix = ""
                if mode == "automatic_retry" and self.launcher.retry_until is not None:
                    detail += "\n" + translate("backoff") + f" · {max(0, self.launcher.retry_until-time.monotonic()):.1f}s"
            elif self.status_key not in ("failure", "startup_blocked", "network_wait", "closed"):
                elapsed = time.monotonic() - self.launcher.phase_at
                detail = translate(self.launcher.phase_key) + f" · {elapsed:.1f}s" + ("\n" + detail if detail else "")
        if supervised:
            since = max(0.0, time.monotonic() - supervised["received_at"])
            detail = translate(supervised.get("key", "supervision")) + f" · {supervised.get('elapsed', 0.0) + since:.1f}s"
            suffix = " · " + translate("unknown_total") + " · " + translate("remaining") + f" {max(0.0, supervised.get('remaining_seconds', 0.0) - since):.1f}s"
            if supervised.get("pid"):
                suffix += " · PID " + str(supervised["pid"])
        self.progress_label.configure(text=detail + suffix, wraplength=max(self.unit*4, self.progress_label.master.winfo_width()))
        if detail:
            self.progress_label.grid()
        else:
            self.progress_label.grid_remove()
        message = translate(self.notice_key) if self.notice_key else ""
        if self.reason:
            fault = self.current_failure.get("fault", {})
            summary = translate("fault_" + fault.get("category", "unknown")) if fault else local_message(self.reason, language).split("\n", 1)[0][:240]
            message = (message + " · " if message else "") + summary
        if self.last_failure and not self.current_failure:
            history = translate("previous_failure") + " · " + (translate("fault_" + self.last_failure["fault"].get("category", "unknown")) if self.last_failure.get("fault") else local_message(self.last_failure.get("reason", ""), language).split("\n", 1)[0][:160])
            message = message + "\n" + history if message else history
        self.notice.configure(text=message)
        if message:
            self.notice.grid()
        else:
            self.notice.grid_remove()

    def pump(self):
        for event in self.mailbox.drain():
            if event.get("generation") == self.generation:
                self.handle(event)
                try:
                    if not self.root.winfo_exists():
                        return
                except self.tk.TclError:
                    return
        if self.closing and self.launcher and not self.launcher.thread.is_alive():
            self.launcher.activity.close()
            self.destroy()
            return
        self.refresh()
        self.root.after(max(1, int(math.sqrt(tick()) * 1000)), self.pump)

    def animate(self):
        now = time.monotonic()
        delta = now - self.last_draw
        self.last_draw = now
        self.phase += delta * math.tau / max(1, math.sqrt(os.cpu_count() or 1))
        self.level *= math.exp(-delta)
        self.orb.delete("all")
        width, height = self.orb.winfo_width(), self.orb.winfo_height()
        center_x, center_y = width/2, height/2
        radius = min(width, height) * .31
        active = self.session_id is not None and not self.ending
        for index in range(3):
            size = radius * (1 + index*.19 + .035*math.sin(self.phase+index))
            self.orb.create_oval(center_x-size, center_y-size, center_x+size, center_y+size, outline=self.palette["accent"] if index == 0 else self.palette["border"], width=2 if index == 0 else 1)
        count = max(5, int(radius/self.unit)*4+1)
        for index in range(count):
            x = center_x + (index-(count-1)/2)*radius*1.4/count
            envelope = max(.1, math.cos((index-(count-1)/2)/count*math.pi))
            movement = (.2 + math.sqrt(self.level)*2) if active else .13
            amplitude = radius * envelope * (movement * (.5+.5*math.sin(self.phase*3+index*.7)) + .08)
            self.orb.create_line(x, center_y-amplitude, x, center_y+amplitude, fill=self.palette["accent"] if active else self.palette["lavender"], width=max(1, self.unit//3), capstyle="round")
        interval = max(tick(), 1 / max(1, min(60, math.sqrt(os.cpu_count() or 1)*10)))
        self.root.after(max(1, int(interval*1000)), self.animate)

    def show_learning(self):
        from tkinter import filedialog, messagebox
        if os.environ.get("YUAN_PUBLISHER") != "1" or not self.workspace or not self.launcher or self.session_id or not self.runtime_ready:
            return
        if self.learning_window and self.learning_window.winfo_exists():
            self.learning_window.lift()
            return
        window = self.tk.Toplevel(self.root)
        self.learning_window = window
        window.title(tr("learning_setup", self.language))
        window.configure(bg=self.palette["panel"])
        window.transient(self.root)
        window.grid_columnconfigure(1, weight=1)
        state = self.readiness_values.get("learning", {})
        missing = "; ".join(f"{row['language']} {row['split']} {row['have']}/{row['need']}" for row in state.get("missing", []))
        detail = tr("metrics_note", self.language) + ("\n" + tr("missing_data", self.language) + ": " + missing if missing else "")
        self.label(window, text=detail, bg=self.palette["panel"], wraplength=min(720, self.root.winfo_screenwidth() - self.unit * 6)).grid(row=0, column=0, columnspan=3, sticky="ew", padx=self.unit, pady=self.unit)
        fields = {}
        keys = ("recording_id", "reviewer", "user_speaker", "assistant_speaker", "license", "user_file", "assistant_file")
        for index, key in enumerate(keys, 1):
            self.label(window, text=tr(key, self.language), bg=self.palette["panel"]).grid(row=index, column=0, sticky="w", padx=self.unit, pady=self.unit // 2)
            value = self.tk.StringVar(window)
            fields[key] = value
            entry = self.tk.Entry(window, textvariable=value, width=36, bg=self.palette["field"], fg=self.palette["text"], insertbackground=self.palette["accent"])
            entry.grid(row=index, column=1, sticky="ew", padx=self.unit)
            if key.endswith("_file"):
                def choose(value=value):
                    filename = filedialog.askopenfilename(parent=window, title=tr("pair_import", self.language), filetypes=[("Audio", "*.wav *.flac *.ogg *.mp3 *.opus *.aiff"), ("All", "*")])
                    if filename:
                        value.set(filename)
                self.button(window, choose, key="choose_audio").grid(row=index, column=2, padx=self.unit)
        language = self.tk.StringVar(window, value=self.language)
        split = self.tk.StringVar(window, value="auto")
        self.label(window, text="中文 / English", bg=self.palette["panel"]).grid(row=8, column=0, padx=self.unit, sticky="w")
        self.ttk.Combobox(window, textvariable=language, values=("中文", "English"), state="readonly").grid(row=8, column=1, sticky="ew", padx=self.unit)
        self.label(window, text=tr("split", self.language), bg=self.palette["panel"]).grid(row=9, column=0, padx=self.unit, sticky="w")
        self.ttk.Combobox(window, textvariable=split, values=("auto", "train", "validation", "guard", "release"), state="readonly").grid(row=9, column=1, sticky="ew", padx=self.unit)
        endings = {}
        ending_values = tuple(tr(key, self.language) for key in ("ending_unknown", "ending_complete", "ending_truncated"))
        for index, role in enumerate(("user", "assistant"), 10):
            endings[role] = self.tk.StringVar(window, value=ending_values[0])
            self.label(window, text=tr(role + "_ending", self.language), bg=self.palette["panel"]).grid(row=index, column=0, padx=self.unit, sticky="w")
            self.ttk.Combobox(window, textvariable=endings[role], values=ending_values, state="readonly", width=40).grid(row=index, column=1, columnspan=2, sticky="ew", padx=self.unit)
        confirmed = self.tk.BooleanVar(window, value=False)
        self.tk.Checkbutton(window, text=tr("human_confirm", self.language), variable=confirmed, bg=self.palette["panel"], fg=self.palette["text"], selectcolor=self.palette["field"], wraplength=640).grid(row=12, column=0, columnspan=3, sticky="w", padx=self.unit, pady=self.unit)
        def submit():
            payload = {key: value.get().strip() for key, value in fields.items()}
            if not confirmed.get() or not all(payload.values()):
                messagebox.showwarning(APP_NAME, tr("required_fields", self.language), parent=window)
                return
            payload.update({role + "_utterance_final": (None, True, False)[ending_values.index(value.get())] for role, value in endings.items()})
            self.launcher.send({"action": "import_pair", **payload, "language": language.get(), "split": split.get(), "confirmed": True,
                                "prompt": self.prompt.get("1.0", "end").strip() or tr("default_prompt", language.get())})
            window.destroy()
        self.button(window, submit, key="pair_import", primary=True).grid(row=13, column=0, columnspan=2, sticky="ew", padx=self.unit, pady=self.unit)
        self.button(window, lambda: self.launcher.send({"action": "review_open"}), key="review_open").grid(row=13, column=2, padx=self.unit, pady=self.unit)
        self.button(window, lambda: self.launcher.send({"action": "export_release"}), key="owned_release_export").grid(row=14, column=0, columnspan=3, padx=self.unit, pady=self.unit)

    def show_review(self, event):
        from tkinter import messagebox
        if self.review_window and self.review_window.winfo_exists():
            self.review_window.destroy()
        evaluation, draft = event["evaluation"], event["review"]
        cases = evaluation["cases"]
        required = ("understandable", "follows_prompt", "relevant_reply", "ends_normally", "no_abnormal_audio")
        window = self.tk.Toplevel(self.root)
        self.review_window = window
        window.title(tr("review_open", self.language))
        window.configure(bg=self.palette["panel"])
        window.transient(self.root)
        window.grid_columnconfigure(1, weight=1)
        labels = {None: tr("review_pending", self.language), True: tr("review_pass", self.language), False: tr("review_fail", self.language)}
        prior = {row["id"]: row for row in draft.get("checks", [])}
        answers = {case["id"]: {key: self.tk.StringVar(window, value=labels[prior.get(case["id"], {}).get(key)]) for key in required} for case in cases}
        position = [0]
        heading = self.label(window, bg=self.palette["panel"], font=self.bold_font)
        heading.grid(row=0, column=0, columnspan=3, sticky="ew", padx=self.unit, pady=self.unit)
        prompt = self.label(window, bg=self.palette["panel"], wraplength=min(680, self.root.winfo_screenwidth() - self.unit * 6))
        prompt.grid(row=1, column=0, columnspan=3, sticky="ew", padx=self.unit)
        selectors = {}
        for index, key in enumerate(required, 3):
            self.label(window, text=tr(key, self.language), bg=self.palette["panel"]).grid(row=index, column=0, sticky="w", padx=self.unit, pady=self.unit // 2)
            selector = self.ttk.Combobox(window, state="readonly", values=tuple(labels.values()))
            selector.grid(row=index, column=1, columnspan=2, sticky="ew", padx=self.unit)
            selectors[key] = selector
        def display():
            case = cases[position[0]]
            heading.configure(text=f"{position[0] + 1} / {len(cases)} · {case['language']} · {evaluation['model']['sha256'][:12]}")
            prompt.configure(text=case["prompt"])
            for key, selector in selectors.items():
                selector.configure(textvariable=answers[case["id"]][key])
        def move(delta):
            position[0] = (position[0] + delta) % len(cases)
            display()
        def play(field):
            self.launcher.send({"action": "review_play", "evaluation_sha256": event["evaluation_sha256"], "case": cases[position[0]]["id"], "field": field})
        self.button(window, lambda: play("input"), key="input").grid(row=2, column=0, padx=self.unit, pady=self.unit)
        self.button(window, lambda: play("output"), key="output").grid(row=2, column=1, padx=self.unit, pady=self.unit)
        self.button(window, lambda: move(-1), text="←").grid(row=8, column=0, padx=self.unit, pady=self.unit)
        self.button(window, lambda: move(1), text="→").grid(row=8, column=2, padx=self.unit, pady=self.unit)
        reviewer = self.tk.StringVar(window, value=draft.get("reviewer", ""))
        self.label(window, text=tr("reviewer", self.language), bg=self.palette["panel"]).grid(row=9, column=0, padx=self.unit)
        self.tk.Entry(window, textvariable=reviewer, bg=self.palette["field"], fg=self.palette["text"], insertbackground=self.palette["accent"]).grid(row=9, column=1, columnspan=2, sticky="ew", padx=self.unit)
        def close():
            if self.launcher:
                self.launcher.send({"action": "review_close"})
            window.destroy()
        def submit():
            if not reviewer.get().strip() or any(value.get() == labels[None] for case in answers.values() for value in case.values()):
                messagebox.showwarning(APP_NAME, tr("review_incomplete", self.language), parent=window)
                return
            checks = [{"id": case_id, **{key: value.get() == labels[True] for key, value in values.items()}} for case_id, values in answers.items()]
            self.launcher.send({"action": "review_submit", "evaluation_sha256": event["evaluation_sha256"], "reviewer": reviewer.get().strip(), "checks": checks})
            window.destroy()
        self.button(window, submit, key="review_submit", primary=True).grid(row=10, column=0, columnspan=3, sticky="ew", padx=self.unit, pady=self.unit)
        window.protocol("WM_DELETE_WINDOW", close)
        display()


def runtime_probe_compute(state, emit, dependencies):
    import io
    np, torch, sf, soxr = (dependencies[name] for name in ("numpy", "torch", "soundfile", "soxr"))
    emit("probe", key="stage_probe_compute", check="owned_network_forward_backward")
    threads = torch.get_num_threads()
    try:
        torch.set_num_threads(max(1, math.isqrt(os.cpu_count() or 1)))
        model = NativeAudio.__new__(NativeAudio)
        model.torch, model.width, model.layers, model.hop = torch, 16, 1, 8
        network = model._build()
        ids = torch.tensor([[1, 2]], dtype=torch.long)
        prompt = network.condition(ids)
        hidden = network.initial(prompt, 1)
        hidden = network.encode(torch.zeros(1, 2, model.hop), hidden, 0)
        output, ending, hidden = network.decode(network.begin, hidden, hidden)
        loss = output.square().mean() + ending.square().mean()
        if not bool(torch.isfinite(loss)):
            raise YuanError("计算检查产生无效数值 / Compute check produced invalid values")
        loss.backward()
        gradients = [value.grad for value in network.parameters() if value.grad is not None]
        if not gradients or not all(bool(torch.isfinite(value).all()) for value in gradients):
            raise YuanError("模型反向计算检查失败 / Model backward check failed")
        for branch in (network.prompt, network.frame_encoder, network.audio_encoder, network.dialogue_core, network.decoder_condition):
            if not any(value.grad is not None and bool(value.grad.abs().sum() > 0) for value in branch.parameters()):
                raise YuanError("模型条件分支梯度检查失败 / Model conditioning gradient check failed")
        emit("probe", key="stage_probe_compute", check="checkpoint_roundtrip")
        with tempfile.TemporaryDirectory(prefix="probe-", dir=os.environ["TMPDIR"]) as directory:
            path = Path(directory) / "weights.pt"
            torch.save(network.state_dict(), path)
            restored = model._build()
            restored.load_state_dict(torch.load(path, map_location="cpu", weights_only=True), strict=True)
            if not all(torch.equal(left, restored.state_dict()[key]) for key, left in network.state_dict().items()):
                raise YuanError("权重读写检查失败 / Checkpoint round-trip check failed")
        emit("probe", key="stage_probe_compute", check="audio_file_resampling")
        values = np.linspace(-0.1, 0.1, 256, dtype=np.float32)
        stream = io.BytesIO()
        sf.write(stream, values, 16000, format="WAV", subtype="FLOAT")
        stream.seek(0)
        recovered, rate = sf.read(stream, dtype="float32")
        resampled = soxr.resample(recovered, rate, rate * 2)
        if not np.array_equal(values, recovered) or not len(resampled) or not np.isfinite(resampled).all():
            raise YuanError("音频读写与重采样检查失败 / Audio file and resampling check failed")
        emit("probe", key="runtime_verify", check="complete")
    finally:
        torch.set_num_threads(threads)

def verify_installed_files(runtime, entry, emit, cancel, budget=None):
    import importlib.metadata as metadata
    import base64
    import csv
    import io
    runtime = Path(runtime).resolve()
    expected = {item["name"]: item["version"] for item in entry["packages"]}
    distributions = {}
    for distribution in metadata.distributions():
        check(cancel)
        name = re.sub(r"[-_.]+", "-", str(distribution.metadata.get("Name", ""))).lower()
        if not name:
            continue
        if name in distributions:
            raise YuanFault("运行组件元数据重复 / Duplicate installed distribution metadata", category="integrity", code="installed_metadata_duplicate", component=name)
        distributions[name] = distribution
    if set(distributions) - set(expected) - {"pip", "setuptools", "wheel"} or any(name not in distributions or distributions[name].version != version for name, version in expected.items()):
        raise YuanFault("运行环境版本与锁定记录不一致 / Runtime versions differ from the lock", category="integrity", code="installed_versions_mismatch")
    checked = unhashed = 0
    last_progress = -math.inf
    period = max(tick(), math.sqrt(tick()))
    with operation(emit, "runtime_inventory", "verification", budget_seconds=budget, stall_seconds=budget) as operation_id:
        for name in expected:
            check(cancel)
            distribution = distributions[name]
            record = distribution.read_text("RECORD")
            if not record:
                raise YuanFault("缺少组件安装文件清单 / Installed file manifest is missing", category="integrity", code="installed_record_missing", component=name)
            hashed = 0
            seen = set()
            for fields in csv.reader(io.StringIO(record)):
                check(cancel)
                if not fields:
                    continue
                if len(fields) != 3 or not fields[0] or fields[0] in seen or fields[2] and not re.fullmatch(r"[0-9]+", fields[2]):
                    raise YuanFault("组件安装清单格式无效 / Invalid installed file manifest", category="integrity", code="installed_record_invalid", component=name)
                item, recorded_hash, recorded_size = fields
                seen.add(item)
                raw = Path(distribution.locate_file(item))
                path = raw.resolve()
                if raw.is_symlink() or not path.is_relative_to(runtime):
                    raise YuanFault("安装文件超出候选运行环境 / Installed file escapes the candidate runtime", category="integrity", code="installed_path_invalid", component=name, file=item)
                if path.suffix == ".pyc" and not recorded_hash:
                    continue
                if not path.is_file():
                    raise YuanFault("组件安装文件缺失 / Installed component file is missing", category="integrity", code="installed_file_missing", component=name, file=item)
                if recorded_size and path.stat().st_size != int(recorded_size):
                    raise YuanFault("组件安装文件大小不符 / Installed component file size mismatch", category="integrity", code="installed_file_size", component=name, file=item)
                if recorded_hash:
                    try:
                        algorithm, expected_hash = recorded_hash.split("=", 1)
                        digest = hashlib.new(algorithm)
                        if digest.digest_size < hashlib.sha256().digest_size:
                            raise ValueError("Weak file hash")
                    except ValueError as exc:
                        raise YuanFault("组件文件哈希算法无效 / Invalid component file hash algorithm", category="integrity", code="installed_hash_algorithm", component=name, file=item) from exc
                    with path.open("rb") as stream:
                        for block in iter(lambda: stream.read(1024 * 1024), b""):
                            check(cancel)
                            digest.update(block)
                    actual = base64.urlsafe_b64encode(digest.digest()).rstrip(b"=").decode("ascii")
                    if actual != expected_hash:
                        raise YuanFault("组件安装文件哈希不匹配 / Installed component file hash mismatch", category="integrity", code="installed_file_hash", component=name, file=item)
                    hashed += 1
                else:
                    unhashed += 1
                checked += 1
                now = time.monotonic()
                if now - last_progress >= period:
                    last_progress = now
                    emit("operation", key="runtime_inventory", scope="verification", operation_id=operation_id,
                         outcome="progress", current=checked, component=name, pid=os.getpid())
            if not hashed:
                raise YuanFault("组件没有可核验的文件哈希 / No verifiable installed file hashes", category="integrity", code="installed_hashes_missing", component=name)
    emit("probe", key="runtime_install_verified", checked_files=checked, unhashed_files=unhashed, packages=len(expected))
    return {"checked_files": checked, "unhashed_files": unhashed, "packages": len(expected)}

def runtime_install_verify(state):
    state = Path(state).resolve()
    with runtime_entry(state) as (emit, cancel):
        try:
            runtime = Path(sys.prefix)
            if not runtime.resolve().is_relative_to(safe_path(state, "runtimes")):
                raise YuanFault("仅允许校验所选目录内的候选环境 / Verification requires a workspace candidate runtime", category="configuration", code="runtime_inventory_location")
            entry = checked_json(runtime / "lock.json")
            budgets = json.loads(os.environ.get("YUAN_IMPORT_BUDGETS", "{}"))
            verify_installed_files(runtime, entry, emit, cancel, budgets.get("runtime_inventory"))
        except BaseException as exc:
            emit("error", key="failure", reason=concise_error(exc), fatal=True, fault=fault_event(exc, "runtime_inventory"))
            raise SystemExit(1) from None

def runtime_probe_import(state, component):
    state = Path(state).resolve()
    with runtime_entry(state) as (emit, cancel):
        try:
            if component not in RUNTIME_MODULES:
                raise YuanFault("无效的组件导入复检目标 / Invalid isolated import target", category="configuration", code="probe_import_target")
            identity = os.environ.get("YUAN_IMPORT_IDENTITY")
            supervisor = StageSupervisor(state, emit, identity=identity if is_digest(identity) else None)
            budgets = json.loads(os.environ.get("YUAN_IMPORT_BUDGETS", "{}"))
            if not isinstance(budgets, dict):
                raise YuanError("导入预算格式无效 / Invalid import budget format")
            emit("runtime_profile", python=platform.python_version(), executable=sys.executable, system=platform.system(), machine=platform.machine(),
                 cwd=str(Path.cwd()), environment=supervisor.fingerprint, mode="isolated_import", component=component)
            import_runtime_module(component, state, emit, cancel, supervisor, budgets)
            supervisor.policy.save()
        except BaseException as exc:
            emit("error", key="failure", reason=concise_error(exc), fatal=True, fault=fault_event(exc, "stage_import_" + component))
            raise SystemExit(1) from None

def runtime_probe(state):
    state = Path(state).resolve()
    with runtime_entry(state) as (emit, cancel):
        try:
            dependencies = load_runtime_modules(state, emit, cancel)
            with operation(emit, "stage_probe_compute"):
                runtime_probe_compute(state, emit, dependencies)
        except BaseException as exc:
            emit("error", key="failure", reason=concise_error(exc), fatal=True, fault=fault_event(exc, "runtime_verify"))
            raise SystemExit(1) from None

def preparation_entry(workspace, publisher=False):
    workspace = Path(workspace).expanduser().resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    state = safe_path(workspace, STATE_NAME)
    env = workspace_environment(workspace)
    os.environ.update(env)
    tempfile.tempdir = env["TMPDIR"]
    if publisher:
        os.environ["YUAN_PUBLISHER"] = "1"
    cancel = threading.Event()
    def emit(kind, **values):
        if kind in ("status", "notice", "error", "deployment", "deployment_preflight"):
            key = values.get("key", kind)
            sys.stdout.write(tr(key, "中文") + " / " + tr(key, "English") + "\n")
            sys.stdout.flush()
    with WorkspaceLease(state / "workspace.lock"):
        owned_release_preflight(state, emit)
        installer = RuntimeInstaller(workspace, cancel, emit)
        interpreter = installer.ensure()
        command = [str(interpreter), str(Path(__file__).resolve()), "--publisher-key" if publisher else "--prepare-release", str(state)]
        child = subprocess.run(command, cwd=state, env=os.environ.copy(), check=False)
        return child.returncode

def prepare_release_entry(state, publisher=False):
    state = Path(state).resolve()
    state.mkdir(parents=True, exist_ok=True)
    env = workspace_environment(state.parent)
    os.environ.update(env)
    tempfile.tempdir = env["TMPDIR"]
    if publisher:
        path = ReleaseTrust(state).initialize_publisher()
        sys.stdout.write(str(path) + "\n")
        return 0
    emit = lambda kind, **values: None
    cancel = threading.Event()
    with WorkspaceLease(state / "engine.lock"):
        owned_release_preflight(state, emit)
        dependencies = load_runtime_modules(state, emit, cancel)
        installer = OwnedReleaseInstaller(state, emit)
        root = installer.prepare(cancel)
        model = NativeAudio(state, cancel, emit, owned_root=root, dependencies=dependencies)
        if root is not None:
            installer.activate(root, model, cancel)
        if model.playback_ready:
            sys.stdout.write("模型验收通过；音频设备将在界面启动时检查 / Model accepted; audio devices will be checked when the interface starts\n")
        else:
            sys.stdout.write("自建模型已初始化并保存；尚未通过对话验收，界面运行时将自动学习 / Native model initialized and saved; conversation not accepted yet, learning runs automatically with the interface\n")
    return 0

def main():
    if len(sys.argv) == 3 and sys.argv[1] in ("--prepare", "--publisher-init", "--prepare-release", "--publisher-key"):
        try:
            if sys.argv[1] in ("--prepare", "--publisher-init"):
                code = preparation_entry(sys.argv[2], publisher=sys.argv[1] == "--publisher-init")
            else:
                code = prepare_release_entry(sys.argv[2], publisher=sys.argv[1] == "--publisher-key")
        except (Exception, KeyboardInterrupt) as exc:
            sys.stderr.write(concise_error(exc) + "\n")
            code = 1
        raise SystemExit(code)
    if len(sys.argv) == 3 and sys.argv[1] == "--publisher":
        os.environ["YUAN_PUBLISHER"] = "1"
        os.environ["YUAN_WORKSPACE"] = str(Path(sys.argv[2]).expanduser().resolve())
    if len(sys.argv) == 3 and sys.argv[1] == "--verify-install":
        runtime_install_verify(sys.argv[2])
        return
    if len(sys.argv) == 4 and sys.argv[1] == "--probe-import":
        runtime_probe_import(sys.argv[2], sys.argv[3])
        return
    if len(sys.argv) == 3 and sys.argv[1] == "--probe":
        runtime_probe(sys.argv[2])
        return
    if len(sys.argv) == 3 and sys.argv[1] == "--engine":
        engine_entry(sys.argv[2])
        return
    try:
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
    except Exception:
        sys.stderr.write("图形界面需要 Python 3.10+、Tkinter 和可用显示环境；无界面准备入口：python Yuan.py --prepare <目录> / The interface requires Python 3.10+, Tkinter and a display; headless preparation: python Yuan.py --prepare <folder>\n")
        raise SystemExit(1)
    try:
        app = YuanApp(root)
        if len(sys.argv) == 3 and sys.argv[1] == "--publisher":
            app.open_workspace(Path(sys.argv[2]).expanduser().resolve())
        root.mainloop()
    except Exception as exc:
        try:
            messagebox.showerror(APP_NAME, concise_error(exc), parent=root)
        finally:
            root.destroy()

if __name__ == "__main__":
    main()
