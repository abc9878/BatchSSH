#!/usr/bin/env python3
import argparse
import csv
import os
import paramiko
import re
import shutil
import socket
import time
import traceback


def get_list():
	path = 'list.csv'
	if not os.path.exists(path):
		print('找不到文件 list.csv')
		return []

	ip_list = []
	# 尝试常见编码，先 utf-8，再回退到 gbk
	for enc in ('utf-8', 'gbk', 'cp936'):
		try:
			with open(path, newline='', encoding=enc) as f:
				reader = csv.reader(f)
				rows = list(reader)
				# 跳过首行(header)，与原来从 Excel 的第 2 行开始一致
				for row in rows[1:]:
					if not row:
						continue
					val = str(row[0]).strip()
					if val:
						ip_list.append(val)
			break
		except UnicodeDecodeError:
			# 尝试下一个编码
			continue
		except Exception as e:
			print('读取 CSV 出错:', e)
			return []

	if not ip_list:
		print('CSV 信息加载完成或为空!!!')
	else:
		print('添加列表如下：')
		for i in ip_list:
			print(i)
	print('合计', len(ip_list), '台主机')
	return ip_list



def get_cmd():
	cmd = ''
	c = ''
	while True:
		cmd_line = input('输入命令，qq回车结束：')
		if cmd_line == 'qq':
			break
		cmd = cmd + cmd_line + '\n'
	if cmd != '':
		print('已输入命令如下：')
		print(cmd)

		cc = input('键入y继续,将自动刷入命令，其它输入退出：')
		if cc != 'y':
			exit()
	else:
		print('未输入任何命令，程序退出！')
		exit()
	return cmd

def put_cmd(iplist, pcmd, dry_run=False):
	if dry_run:
		print('DRY RUN: 将不会建立 SSH 连接，仅打印将要执行的命令')
		for ip in iplist:
			print('-----')
			print('目标：', ip)
			print('命令：')
			print(pcmd)
			# 记录 dry-run 日志，包含将要执行的命令作为详细信息
			write_log(ip, 'DRY_RUN', '仅模拟执行，未建立连接', details=pcmd)
		print('DRY RUN 完成，未进行任何网络连接')
		return

	un = input('输入用户名：')
	pd = input('密码：')
	for ip in iplist:
		try:
			ssh = paramiko.SSHClient()
			ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			ssh.connect(ip,22,un,pd,timeout=2)
			ssh_cmd = ssh.invoke_shell()
			time.sleep(1)
			start_ts = time.time()
			ssh_cmd.send(pcmd)
			# 尝试读取短时间内的返回以便记录输出（非交互式抓取，有限时间）
			output = ''
			deadline = time.time() + 3.0
			while time.time() < deadline:
				try:
					if ssh_cmd.recv_ready():
						chunk = ssh_cmd.recv(65535)
						if not chunk:
							break
						try:
							output += chunk.decode('utf-8', errors='replace')
						except Exception:
							output += chunk.decode('gbk', errors='replace')
					else:
						time.sleep(0.2)
				except Exception:
					break
			end_ts = time.time()
			duration = end_ts - start_ts
			ssh.close()
			print(ip,'完成！')
			# 记录成功，包含耗时与部分输出（如有）
			write_log(ip, 'SUCCESS', f'命令已发送，耗时 {duration:.2f}s', details=output)
		except Exception as e:
			err_trace = traceback.format_exc()
			print(ip)
			print('用户名密码错误或者连接超时')
			# 识别是否为认证失败或连接失败；如果是，则在日志 details 中屏蔽 traceback
			try:
				from paramiko import AuthenticationException, ssh_exception
				auth_exc = isinstance(e, AuthenticationException)
			except Exception:
				auth_exc = False

			is_link_fail = False
			try:
				from paramiko import ssh_exception
				if isinstance(e, ssh_exception.NoValidConnectionsError):
					is_link_fail = True
			except Exception:
				pass

			if isinstance(e, socket.timeout) or isinstance(e, ConnectionRefusedError) or isinstance(e, OSError):
				is_link_fail = True

			# 如果是连接或认证失败，则在日志 details 中写入简短错误类型，避免泄露执行堆栈
			if auth_exc or is_link_fail:
				# 写入简短错误类型到日志 details
				if auth_exc:
					short_msg = 'Authentication failed.'
				elif is_link_fail:
					short_msg = 'Connection failed.'
				else:
					short_msg = 'Connection or authentication failed.'
				write_log(ip, 'FAILED', str(e), details=short_msg)
			else:
				# 其它异常保留完整 traceback 以便排查
				write_log(ip, 'FAILED', str(e), details=err_trace)
			# 仍然将该 IP 追加到对应的失败 CSV 以便后续处理
			append_error_ip(ip, e)
			continue


def write_log(ip, status, message, details=''):
	"""将执行结果追加到 log.txt（GBK 编码），包含时间、IP、状态、消息和可选详情。"""
	now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
	# 这里写入时使用统一的 LF ('\n')，二进制归一化会在写入后转换为系统换行（如 CRLF）
	NL = '\n'
	header = f"[{now}] {ip} | {status} | {message}" + NL
	try:
		# 强制使用在 __main__ 中设置的 LOG_FILE；若未设置则跳过写日志
		logfile = globals().get('LOG_FILE')
		if not logfile:
			print('LOG_FILE 未设置，跳过写入日志')
			return
		# 使用 GBK 编码写入以便在 Windows 下用记事本查看中文正常
		# 构造条目文本，统一使用 LF 作为行分隔符，最后确保每条主机条目以两个 LF 结尾（形成一个空行分隔）
		parts = [header.rstrip('\n')]
		if details:
			# 规范 details 中的换行：将任意连续的 CR/LF 序列压缩为单个 LF
			# 并去掉首尾空行以避免产生多余空行
			normalized = re.sub(r'[\r\n]+', '\n', details)
			normalized = normalized.strip()
			# 限制详情长度以免日志过大（基于规范化后的文本）
			normalized = normalized[:10000]
			parts.append('DETAILS:')
			parts.append(normalized)
		entry_text = NL.join(parts) + NL + NL
		# 以二进制方式写入 LF 字节，避免文本模式下自动换行转换带来的不确定性
		entry_bytes = entry_text.encode('cp936', errors='replace')
		with open(logfile, 'ab') as _bf:
			_bf.write(entry_bytes)
		# 写入完成后再做一次二进制级别的换行归一化，防止不同写入路径组合出重复的 CR 字节
		try:
			# 以二进制方式读取并对换行进行温和归一化：
			# 1) 将 CRLF/CR/ LF 统一为单一的 LF 字节（\n），保留原有的空行数量；
			# 2) 去掉开头/结尾的多余空行；
			# 3) 将 LF 转换为目标系统换行字节（通常为 CRLF），从而保留中间的空行数目。
			with open(logfile, 'rb') as _bf:
				_data = _bf.read()
			_nl = os.linesep.encode('ascii')
			# 先把 CRLF -> LF，再把孤立 CR -> LF，得到纯 LF 流
			_tmp = _data.replace(b'\r\n', b'\n')
			_tmp = _tmp.replace(b'\r', b'\n')
			# 在每个日志条目的前面（即下一行以 [YYYY- 开头的地方）插入一个额外的 LF，
			# 以确保主机之间有一个空行分隔。这里用正则只匹配时间戳格式的 header 行。
			try:
				_tmp = re.sub(b'\n(?=\[\d{4}-\d{2}-\d{2} )', b'\n\n', _tmp)
			except Exception:
				# 如果正则失败则回退到简单压缩逻辑
				_tmp = re.sub(b'\n{3,}', b'\n\n', _tmp)
			# 将任意超过两个连续 LF 的序列压缩为两个 LF（保持 host 间的单个空行，去除冗余行）
			_tmp = re.sub(b'\n{3,}', b'\n\n', _tmp)
			# 去掉文件首尾多余的空行（LF），保留中间空行数量（即两个 LF 表示空行分隔）
			_tmp = _tmp.strip(b'\n')
			# 将 LF 替换为目标换行字节（保留双 LF 导致的空行）
			_normalized = _tmp.replace(b'\n', _nl)
			# 确保以单个换行结尾
			_normalized = _normalized + _nl
			with open(logfile, 'wb') as _bf:
				_bf.write(_normalized)
		except Exception:
			# 忽略二进制归一化错误
			pass
	except Exception:
		# 忽略日志写入错误，但在控制台输出以便注意
		print('无法写入日志文件', logfile if logfile else 'None')


def append_error_ip(ip, exc):
	"""根据异常类型将失败的 IP 追加到不同的 CSV 文件：
	- 无法连接 (socket.timeout, NoValidConnectionsError, OSError 等) -> linkfail.csv
	- 认证失败 (paramiko.AuthenticationException) -> loginfail.csv
	如果无法识别，默认写入 linkfail.csv。"""
	message = str(exc)
	# 识别认证失败
	try:
		from paramiko import AuthenticationException, ssh_exception
		auth_exc = isinstance(exc, AuthenticationException)
	except Exception:
		auth_exc = False

	# 识别连接失败
	is_link_fail = False
	try:
		from paramiko import ssh_exception
		if isinstance(exc, ssh_exception.NoValidConnectionsError):
			is_link_fail = True
	except Exception:
		pass

	if isinstance(exc, socket.timeout) or isinstance(exc, ConnectionRefusedError) or isinstance(exc, OSError):
		is_link_fail = True

	# 选择目标文件
	if auth_exc:
		target = 'loginfail.csv'
		header = ['ip', 'error']
	else:
		target = 'linkfail.csv'
		header = ['ip', 'error']

	write_header = not os.path.exists(target)
	now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
	try:
		# 使用 GBK 编码写入以便在 Windows 下用记事本查看中文正常
		with open(target, 'a', newline='', encoding='cp936', errors='replace') as ef:
			writer = csv.writer(ef)
			if write_header:
				# header includes timestamp column
				writer.writerow(header + ['time'])
			writer.writerow([ip, message, now])
	except Exception:
		print('无法写入', target)

if __name__ == '__main__':
	# 为本次执行设定唯一的日志文件名，并放入 logs 子目录，格式：YYYY-MM-DD-HHMMSS.log
	logs_dir = 'logs'
	try:
		os.makedirs(logs_dir, exist_ok=True)
	except Exception:
		print('无法创建 logs 目录，日志将写入当前目录')
		logs_dir = '.'
	LOG_FILE = os.path.join(logs_dir, f"{time.strftime('%Y-%m-%d')}-{time.strftime('%H%M%S')}.log")

	# 每次执行前将旧的 linkfail/loginfail CSV 移入 Archive 子目录并追加时间戳（格式 YYMMDD-hhmmss）
	def archive_csv(filename):
		if not os.path.exists(filename):
			return
		archive_dir = os.path.join('logs', 'archive')
		try:
			os.makedirs(archive_dir, exist_ok=True)
		except Exception:
			print('无法创建 logs/archive 目录，跳过归档')
			return
		# 时间戳格式改为 YYMMDD-hhmmss
		ts = time.strftime('%y%m%d-%H%M%S', time.localtime())
		base = os.path.splitext(os.path.basename(filename))[0]
		newname = f"{base}_{ts}.csv"
		dest = os.path.join(archive_dir, newname)
		try:
			shutil.move(filename, dest)
		except Exception as e:
			print('归档失败', filename, e)

	# 归档可能存在的旧 CSV
	archive_csv('linkfail.csv')
	archive_csv('loginfail.csv')

	parser = argparse.ArgumentParser(description='批量刷配置脚本，读取 list.csv')
	parser.add_argument('--dry-run', action='store_true', help='仅打印将执行的命令，不建立 SSH 连接')
	args = parser.parse_args()

	iplist = get_list()
	if not iplist:
		print('没有可用的主机，程序退出')
		exit()
	cmd = get_cmd()
	put_cmd(iplist, cmd, dry_run=args.dry_run)
